defmodule Craft.LinearizabilityTest do
  use Craft.NexusCase

  alias Craft.Nexus.Stability

  @tag timeout: :timer.minutes(5)
  nexus_test "under ideal conditions", %{nodes: nodes, name: name, nexus: nexus} do
    wait_until(nexus, {Stability, :all})

    history =
      1..20
      |> Task.async_stream(fn _ -> do_random_commands(name, nodes) end, timeout: :infinity)
      |> Enum.flat_map(fn {:ok, ops} -> ops end)

    IO.puts length(history)

    :timer.tc(fn ->
      assert Craft.Linearizability.linearizable?(history, Craft.SimpleMachine)
    end, :second) |> IO.inspect

    # File.write!("history", :erlang.term_to_binary(history))
  end

  defp do_random_commands(name, nodes) do
    for i <- 1..20 do
      command =
        Enum.random([
          {:put, :some_key, "#{inspect self()}_#{i}"},
          {:get, :some_key}
        ])

      Process.sleep(:rand.uniform(50))

      do_command(name, nodes, command)
    end
  end

  defp do_command(name, nodes, command) do
    called_at = :erlang.monotonic_time()
    response = Craft.command(command, name, nodes)
    received_at = :erlang.monotonic_time()

    %{
      client: self(),
      called_at: called_at,
      received_at: received_at,
      command: command,
      response: response
    }
  end
end
