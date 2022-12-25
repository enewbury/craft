defmodule Craft.TestHelper do
  alias Craft.Consensus
  alias Craft.Consensus.FollowerState
  alias Craft.Log.MapLog
  alias Craft.SimpleMachine

  def start_group(states_or_nodes, machine \\ SimpleMachine)

  def start_group([{_node, _state} | _] = states, machine) do
    name =
      :crypto.strong_rand_bytes(3)
      |> Base.encode16()
      |> String.to_atom()

    nodes = Keyword.keys(states)

    {:ok, nexus} = Craft.Nexus.start_link(nodes)

    states =
      Enum.map(states, fn {node, state} ->
        {node, %{state |
                 name: name,
                 other_nodes: List.delete(nodes, node),
                 nexus_pid: nexus}}
      end)

    for node <- nodes do
      :pong = Node.ping(node)
      {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    end

    machine_args = %{
      name: name,
      machine: machine
    }
    # ensure all members are up and ready
    Task.async_stream(states, fn {node, state} ->
      {:ok, _pid} = :rpc.call(node, Craft, :start_member, [name, nodes, SimpleMachine, %{consensus_state: state, machine_args: machine_args}])
    end)
    |> Stream.run()

    Task.async_stream(nodes, fn node ->
      :gen_statem.cast({Consensus.name(name), node}, :run)
    end)
    |> Stream.run()

    {:ok, name, nexus}
  end

  def start_group(nodes, machine) do
    nodes
    |> Enum.map(fn node ->
      {node, %FollowerState{log: Craft.Log.new(nil, MapLog)}}
    end)
    |> start_group(machine)
  end
end


ExUnit.start()
