defmodule Craft.TestHelper do
  alias Craft.Consensus

  def start_group(states, nexus) do
    name =
      :crypto.strong_rand_bytes(3)
      |> Base.encode16()

    nodes = Keyword.keys(states)

    states =
      Enum.map(states, fn {node, state} ->
        {node, %{state | name: name, other_nodes: List.delete(nodes, node), tracer_pid: nexus}}
      end)

    for node <- nodes do
      :pong = Node.ping(node)
      {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    end

    # ensure all members are up and ready
    Task.async_stream(states, fn {node, state} ->
      :rpc.call(node, Craft.Application, :start_member, [state])
    end)
    |> Stream.run()

    Task.async_stream(nodes, fn node ->
      :gen_statem.cast({Consensus.name(name), node}, :run)
    end)
    |> Stream.run()

    name
  end
end


ExUnit.start()
