defmodule Craft do
  alias Craft.Consensus
  alias Craft.Log.MapLog

  # FIXME
  @type command :: any()
  @type reply :: any()
  @type side_effects :: any()
  @type group_name :: any()
  @type query() :: any()
  @type log_index() :: non_neg_integer()

  def start_group(name, nodes, machine, opts \\ [log_module: MapLog]) do
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

    :ok = with_leader_redirect(name, cluster_nodes, &Consensus.add_member(name, &1, node))

    #
    # the nodes we provide to the new member here will eventually be overwritten when
    # the new member processes the MembershipEntry as it catches up to the leader
    #
    # TODO: be ok with the member already being started (maybe it wasn't able to catch up fast enough last time)
    #
    {:ok, _pid} = :rpc.call(node, Craft, :start_member, [name, members.voting_nodes, machine_module, [log_module: log_module]])
  end

  defdelegate start_member(name, nodes, machine, opts), to: Craft.MemberSupervisor
  defdelegate stop_member(name), to: Craft.MemberSupervisor

  def command(command, name, nodes, opts \\ []) do
    with_leader_redirect(name, nodes, &Consensus.command(name, &1, command), opts)
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

    # let the user deal with redirecting the request to the leader, we may implement
    # redirection strategies later (e.g.  try all nodes, redirect N times, etc...)
    case func.(node) do
      {:error, {:not_leader, leader}} ->
        Craft.LeaderCache.put(name, leader)

        if redirect_once do
          opts = Keyword.put(opts, :redirect_once, false)
          with_leader_redirect(name, nodes, func, opts)
        else
          {:error, {:not_leader, leader}}
        end

      reply ->
        reply
    end
  end

  def step_down(name, node) do
    :gen_statem.cast({Consensus.name(name), node}, :step_down)
  end

  defdelegate start_dev_test_cluster(num \\ 5), to: Craft.Test.ClusterNodes, as: :spawn_nodes

  def start_dev_consensus_group(nodes) do
    name = :crypto.strong_rand_bytes(3) |> Base.encode16()

    start_group(name, nodes, Craft.SimpleMachine)

    name
  end

  def state(name, nodes) do
    {:ok, %{members: members}} = with_leader_redirect(name, nodes, &Consensus.configuration(name, &1))

    members.voting_nodes
    |> MapSet.union(members.non_voting_nodes)
    |> Enum.into(%{}, fn node ->
      {node,
       consensus: {Consensus.name(name), node} |> :sys.get_state(),
       machine: {Craft.Machine.name(name), node} |> :sys.get_state()}
    end)
  end
end
