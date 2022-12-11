defmodule Craft do
  alias Craft.Consensus
  alias Craft.Log.MapLog

  #FIXME
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
    #TODO: ask group for its nodes instead of relying on user to pass in the correct set of nodes

    for node <- nodes do
      :ok = :rpc.call(node, Craft, :stop_member, [name])
    end
  end

  defdelegate start_member(name, nodes, machine, opts), to: Craft.MemberSupervisor
  defdelegate stop_member(name), to: Craft.MemberSupervisor

  def command(command, name, nodes, opts \\ []) do
    redirect_once = Keyword.pop(opts, :redirect_once, true)

    node =
      case Craft.LeaderCache.get(name) do
        {:ok, leader} ->
          leader

        :not_found ->
          Enum.random(nodes)
      end

    # let the user deal with redirecting the request to the leader, we may implement
    # redirection strategies later (e.e.  try all nodes, redirect N times, etc...)
    case Consensus.command(name, node, command) do
      {:error, {:not_leader, leader}} ->
        Craft.LeaderCache.put(name, leader)
        if redirect_once do
          opts = Keyword.put(opts, :redirect_once, false)
          command(command, name, nodes, opts)
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
    :crypto.strong_rand_bytes(3)
    |> Base.encode16()
    |> start_group(nodes, Craft.SimpleMachine)

    :ok
  end

  def state(name, nodes) do
    Enum.into(nodes, %{}, fn node ->
      {
        {node, {Consensus.name(name), node} |> :sys.get_state()},
        {node, {Craft.Machine.name(name), node} |> :sys.get_state()}
      }
    end)
  end
end
