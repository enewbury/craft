defmodule Craft.TestGroup do
  alias Craft.Consensus
  alias Craft.Persistence
  alias Craft.SimpleMachine
  alias Craft.NexusCase.Formatter

  def start_group(states_or_nodes, opts \\ %{})

  def start_group([{_node, _state} | _] = states, opts) do
    name = group_name()

    nodes = Keyword.keys(states)

    {:ok, nexus} = Craft.Nexus.start(nodes, self())

    states =
      Enum.map(states, fn {node, state} ->
        {node, %{state |
                 current_term: Persistence.latest_term(state.persistence),
                 name: name,
                 nexus_pid: nexus}}
      end)

    prepare_nodes(nodes)

    Task.async_stream(states, fn {node, state} ->
      opts = %{
        machine: opts[:machine] || SimpleMachine,
        consensus_state: state,
        nodes: nodes,
        nexus_pid: nexus,
        global_clock: opts[:global_clock],
        manual_start: true
      }

      {:ok, _pid} = :rpc.call(node, Craft.MemberSupervisor, :start_member, [name, opts])
    end)
    |> Stream.run()

    Craft.discover(name, nodes)

    Formatter.register(nexus, Process.get(:test_id))

    run(name, nodes)

    {:ok, name, nexus}
  end

  def start_group(nodes, opts) do
    name = group_name()

    {:ok, nexus} = Craft.Nexus.start(nodes, self())

    prepare_nodes(nodes)

    Task.async_stream(nodes, fn node ->
      opts = %{
        machine: opts[:machine] || SimpleMachine,
        nodes: nodes,
        nexus_pid: nexus,
        global_clock: opts[:global_clock],
        manual_start: true
      }

      {:ok, _pid} = :rpc.call(node, Craft.MemberSupervisor, :start_member, [name, opts])
    end)
    |> Stream.run()

    Craft.discover(name, nodes)

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
