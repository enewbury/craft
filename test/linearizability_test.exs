defmodule Craft.LinearizabilityTest do
  use Craft.NexusCase

  alias Craft.Nexus.Stability

  nexus_test "under ideal conditions", %{nodes: nodes, name: name, nexus: nexus} do
    wait_until(nexus, {Stability, :all})

    history =
    0..20
    |> Task.async_stream(fn _ ->
      do_random_commands(name, nodes)
    end)
    |> Enum.flat_map(fn {:ok, ops} -> ops end)

    File.write!("history", :erlang.term_to_binary(history))
  end

  defp do_random_commands(name, nodes) do
    for i <- 0..20 do
      command =
        Enum.random([
          {:put, :some_key, "#{inspect self()}_#{i}"},
          {:get, :some_key}
        ])

      Process.sleep(:rand.uniform(100))
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
