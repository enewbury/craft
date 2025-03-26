defmodule Craft.TestGroup do
  alias Craft.Consensus
  alias Craft.Persistence
  alias Craft.Persistence.RocksDBPersistence
  # alias Craft.Persistence.MapPersistence
  alias Craft.SimpleMachine
  alias Craft.NexusCase.Formatter

  def start_group(states_or_nodes, machine \\ SimpleMachine)

  def start_group([{_node, _state} | _] = states, machine) do
    name = group_name()

    nodes = Keyword.keys(states)

    {:ok, nexus} = Craft.Nexus.start(nodes)

    states =
      Enum.map(states, fn {node, state} ->
        {node, %{state |
                 current_term: Persistence.latest_term(state.persistence),
                 name: name,
                 nexus_pid: nexus}}
      end)

    prepare_nodes(nodes)

    machine_args = %{
      name: name,
      machine: machine
    }

    Task.async_stream(states, fn {node, state} ->
      {:ok, _pid} = :rpc.call(node, Craft, :start_member, [name, nodes, SimpleMachine, %{consensus_state: state, machine_args: machine_args}])
    end)
    |> Stream.run()

    Formatter.register(nexus, Process.get(:test_id))

    run(name, nodes)

    {:ok, name, nexus}
  end

  def start_group(nodes, machine) do
    name = group_name()

    {:ok, nexus} = Craft.Nexus.start(nodes)

    prepare_nodes(nodes)

    args = %{
      name: name,
      nodes: nodes,
      machine: machine,
      persistence: {RocksDBPersistence, []},
      nexus_pid: nexus
    }

    Task.async_stream(nodes, fn node ->
      {:ok, _pid} = :rpc.call(node, Craft, :start_member, [name, nodes, SimpleMachine, args])
    end)
    |> Stream.run()

    Formatter.register(nexus, Process.get(:test_id))

    run(name, nodes)

    {:ok, name, nexus}
  end

  defp group_name do
    :crypto.strong_rand_bytes(3)
    |> Base.encode16()
  end

  defp prepare_nodes(nodes) do
    for node <- nodes do
      :pong = Node.ping(node)
      {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    end
  end

  def run(name, nodes) do
    Task.async_stream(nodes, fn node ->
      :gen_statem.cast({Consensus.name(name), node}, :run)
    end)
    |> Stream.run()
  end
end
