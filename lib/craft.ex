#
# commands and queries are handled differently
#
# commands modify state, so they're automatically directed to the leader to go through a consensus round
#
# queries read state, their handling depends on the level of consistency desired:
#   - linearizable queries are always directed to the leader
#     - if leader leases are enabled (you're using an accurate time source), the leader's machine can immediately process the request
#     - otherwise, we need to wait for a round of consenus to occur to confirm that the leader is still the leader
#       - when a client makes a query, the machine process simply buffers it
#       - when the next consensus round achieves consensus, the consensus process informs the machine process of the (possibly bumped) commit index
#       - the machine process flushes the buffer and processes all waiting queries
#
#  - eventually consistent queries are immediately processed by the receiving node (random node or one provided by the user)
#    - if the user consistently uses the same node, monotonic read consistency is supported
#    - otherwise, if the user leaves it up to craft to pick a random node, there are no consistency guarantees, state can be arbitrarily stale, and even violate causality
#
defmodule Craft do
  alias Craft.Consensus
  alias Craft.Machine
  alias Craft.MemberCache
  alias Craft.Consensus.State.Members

  require Logger

  # FIXME
  @type group_name :: any()
  @type command :: any()
  @type reply :: any()
  @type side_effects :: any()
  @type query() :: any()
  @type log_index() :: non_neg_integer()

  # opts: [:log_module | :data_dir, ...]
  def start_group(name, nodes, machine, opts \\ []) do
    for node <- nodes do
      :pong = Node.ping(node)
      {:module, __MODULE__} = :rpc.call(node, Code, :ensure_loaded, [__MODULE__])
    end

    for node <- nodes do
      {:ok, _pid} = :rpc.call(node, __MODULE__, :start_member, [name, nodes, machine, opts])
    end
  end

  def stop_group(name) do
    case with_leader_redirect(name, &configuration(name, &1)) do
      {:ok, %{members: members}} ->
        results =
          members
          |> Members.all_nodes()
          |> Map.new(fn node ->
            result =
              try do
                :rpc.call(node, __MODULE__, :stop_member, [name])
              catch :exit, e ->
                e
              end

            {node, result}
          end)

        if Enum.all?(results, fn {_node, result} -> result == :ok end) do
          :ok
        else
          results
        end

      error ->
        error
    end
  end

  def add_member(name, node) do
    :pong = Node.ping(node)

    {:ok, config} = with_leader_redirect(name, &configuration(name, &1))

    {%{
       members: members,
       machine_module: machine_module
     }, opts} = Map.split(config, [:members, :machine_module])

    for module <- [__MODULE__, machine_module, Map.get(opts, :log_module)] do
      {:module, ^module} = :rpc.call(node, Code, :ensure_loaded, [module])
    end

    {:ok, _pid} = :rpc.call(node, __MODULE__, :start_member, [name, members.voting_nodes, machine_module, Keyword.new(opts)])

    with_leader_redirect(name, &Consensus.add_member(name, &1, node))

    #
    # the nodes we provide to the new member here will eventually be overwritten when
    # the new member processes the MembershipEntry as it catches up to the leader
    #
    # TODO: be ok with the member already being started (maybe it wasn't able to catch up fast enough last time)
    #
  end

  def remove_member(name, node) do
    with_leader_redirect(name, &Consensus.remove_member(name, &1, node))
  end

  def transfer_leadership(name, to_node) do
    with_leader_redirect(name, &Consensus.transfer_leadership(name, &1, to_node))
  end

  defdelegate start_member(name, nodes, machine, opts), to: Craft.MemberSupervisor
  defdelegate stop_member(name), to: Craft.MemberSupervisor
  defdelegate discover(name, nodes), to: Craft.MemberCache

  def command(command, name, opts \\ []) do
    with_leader_redirect(name, &Machine.command(name, &1, command, opts))
  end

  #
  # Craft.query(command, name, consistency: :linearizable)
  # if `consistency` is `:linearizable`, will address leader
  # if `consistency` is `:eventual`, will address random follower
  # if `consistency` is `{:eventual, node}`, will address given node
  #
  # Craft.command(command, name) # always goes to leader, since it can modify state
  #
  # TODO: timeout in opts
  #       let user choose if query handler in Machine.handle_cast(:query) spawns off a new process, to unblock the machine
  #         - it'll copy the machine's state to a new process, which could be worth it in some scenarios
  #

  def query(query, name, opts \\ []) do
    consistency = Keyword.get(opts, :consistency, :linearizable)

    case consistency do
      :linearizable ->
        with_leader_redirect(name, &Machine.query(name, &1, query, consistency))

      {:eventual, node} ->
        Machine.query(name, node, query, :eventual)

      :eventual ->
        case MemberCache.get(name) do
          {:ok, _leader, members} ->
            node = Enum.random(members)

            Machine.query(name, node, query, consistency)

          :not_found ->
            Logger.error("No known nodes for group '#{inspect(name)}', have you called Craft.discover/2?")

            {:error, :unknown_group}
        end
    end
  end

  defp with_leader_redirect(name, func) do
    case MemberCache.get(name) do
      {:ok, nil, members} ->
        do_leader_redirect(name, Enum.random(members), members, func)

      {:ok, leader, members} ->
        do_leader_redirect(name, leader, members, func)

      :not_found ->
        Logger.error("No known nodes for group '#{inspect(name)}', have you called Craft.discover/2?")

        {:error, :unknown_group}
    end
  end

  defp do_leader_redirect(name, leader, members, func, previous_redirects \\ MapSet.new()) do
    case func.(leader) do
      {:error, :unknown_leader} ->
        members = MapSet.delete(members, leader)

        if Enum.empty?(members) do
          {:error, :unknown_leader}
        else
          do_leader_redirect(name, Enum.random(members), members, func)
        end

      {:error, {:not_leader, leader}} ->
        if MapSet.member?(previous_redirects, leader) do
          {:error, :redirect_loop}
        else
          MemberCache.update_leader(name, leader)

          do_leader_redirect(name, leader, members, func, previous_redirects)
        end

      reply ->
        reply
    end
  end

  def step_down(name, node) do
    :gen_statem.cast({Consensus.name(name), node}, :step_down)
  end

  def start_dev_cluster(num \\ 5) do
    {name, nodes} =
      num
      |> Craft.TestCluster.spawn_nodes()
      |> start_dev_consensus_group()

    discover(name, nodes)

    name
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

  @doc false
  def state(name, node) do
    try do
      {node,
       consensus: Consensus.state(name, node),
       machine: Machine.state(name, node)}
    catch :exit, e ->
      {node, e}
    end
  end

  @doc false
  def state(name) do
    {:ok, %{members: members}} = with_leader_redirect(name, &configuration(name, &1))

    members.voting_nodes
    |> MapSet.union(members.non_voting_nodes)
    |> Enum.into(%{}, &state(name, &1))
  end

  defp configuration(name, node) do
    try do
      Consensus.configuration(name, node)
    catch :exit, e ->
      {:error, e}
    end
  end
end
