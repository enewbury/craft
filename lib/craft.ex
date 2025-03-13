defmodule Craft do
  alias Craft.Consensus
  alias Craft.Machine

  # FIXME
  @type command :: any()
  @type reply :: any()
  @type side_effects :: any()
  @type group_name :: any()
  @type query() :: any()
  @type log_index() :: non_neg_integer()

  # opts: [:log_module | :data_dir, ...]
  def start_group(name, nodes, machine, opts \\ []) do
    for node <- nodes do
      :pong = Node.ping(node)
      {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    end

    for node <- nodes do
      {:ok, _pid} = :rpc.call(node, Craft, :start_member, [name, nodes, machine, opts])
    end
  end

  def stop_group(name, nodes) do
    # TODO: ask group for its nodes instead of relying on user to pass in the correct set of nodes
    for node <- nodes do
      :ok = :rpc.call(node, Craft, :stop_member, [name])
    end
  end

  def add_member(name, node, cluster_nodes) do
    :pong = Node.ping(node)

    {:ok,
     %{
       members: members,
       machine_module: machine_module,
       log_module: log_module
     }} = with_leader_redirect(name, cluster_nodes, &Consensus.configuration(name, &1))

    {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    {:module, ^machine_module} = :rpc.call(node, Code, :ensure_loaded, [machine_module])
    {:module, ^log_module} = :rpc.call(node, Code, :ensure_loaded, [log_module])

    {:ok, _pid} = :rpc.call(node, Craft, :start_member, [name, members.voting_nodes, machine_module, [log_module: log_module]])

    with_leader_redirect(name, cluster_nodes, &Consensus.add_member(name, &1, node))

    #
    # the nodes we provide to the new member here will eventually be overwritten when
    # the new member processes the MembershipEntry as it catches up to the leader
    #
    # TODO: be ok with the member already being started (maybe it wasn't able to catch up fast enough last time)
    #
  end

  def remove_member(name, node, cluster_nodes) do
    with_leader_redirect(name, cluster_nodes, &Consensus.remove_member(name, &1, node))
  end

  def transfer_leadership(name, to_node, cluster_nodes) do
    with_leader_redirect(name, cluster_nodes, &Consensus.transfer_leadership(name, &1, to_node))
  end

  defdelegate start_member(name, nodes, machine, opts), to: Craft.MemberSupervisor
  defdelegate stop_member(name), to: Craft.MemberSupervisor

  def command(command, name, nodes, opts \\ []) do
    with_leader_redirect(name, nodes, &Machine.command(name, &1, command), opts)
  end

  defp with_leader_redirect(name, nodes, func, opts \\ []) do
    redirect_once = Keyword.pop(opts, :redirect_once, true)

    node =
      case Craft.LeaderCache.get(name) do
        {:ok, leader} ->
          leader

        :not_found ->
          Enum.random(nodes)
      end

    case func.(node) do
      {:error, :unknown_leader} ->
        remaining_nodes = List.delete(nodes, node)

        if Enum.empty?(remaining_nodes) do
          {:error, :unknown_leader}
        else
          with_leader_redirect(name, remaining_nodes, func, opts)
        end

      {:error, {:not_leader, leader}} ->
        Craft.LeaderCache.put(name, leader)

        if redirect_once do
          opts = Keyword.put(opts, :redirect_once, false)
          with_leader_redirect(name, nodes, func, opts)
        else
          {:error, {:not_leader, leader}}
        end

      reply ->
        Craft.LeaderCache.put(name, node)

        reply
    end
  end

  def step_down(name, node) do
    :gen_statem.cast({Consensus.name(name), node}, :step_down)
  end

  def start_dev_cluster(num \\ 5) do
    num
    |> Craft.TestCluster.spawn_nodes()
    |> start_dev_consensus_group()
  end

  def start_tmux_cluster do
    nodes = for i <- 2..5, do: :"#{i}@127.0.0.1"
    name = "abc"

    start_group(name, nodes, Craft.RocksDBMachine)

    {name, nodes}
  end

  def start_dev_consensus_group(nodes) do
    name = :crypto.strong_rand_bytes(3) |> Base.encode16()

    start_group(name, nodes, Craft.SimpleMachine)

    {name, nodes}
  end

  def state(name, nodes) do
    {:ok, %{members: members}} = with_leader_redirect(name, nodes, &Consensus.configuration(name, &1))

    members.voting_nodes
    |> MapSet.union(members.non_voting_nodes)
    |> Enum.into(%{}, fn node ->
      try do
        {node,
         consensus: Consensus.state(name, node),
         machine: Machine.state(name, node)}
      catch :exit, e ->
          {node, e}
      end
    end)
  end
end
