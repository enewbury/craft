defmodule Craft.LinearizabilityTest do
  use Craft.NexusCase

  alias Craft.Nexus.Stability
  alias Craft.ParallelClients
  alias Craft.Linearizability

  @moduletag timeout: :timer.minutes(20)

  nexus_test "under stable conditions", ctx do
    wait_until(ctx.nexus, {Stability, :all})
    num_clients = 10
    num_commands = 100

    assert {:ok, _linearized_history, _ignored_ops = []} =
             ctx
             |> random_command_fun()
             |> ParallelClients.run(num_clients, num_commands)
             |> Linearizability.linearize(Craft.SimpleMachine)
  end

  nexus_test "during leadership transfer", %{nodes: nodes, name: name, nexus: nexus} = ctx do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    num_clients = 10

    clients =
      ctx
      |> random_command_fun()
      |> ParallelClients.start(num_clients)

    Process.sleep(300)

    new_leader = Enum.random(nodes -- [leader])
    :ok = Craft.transfer_leadership(name, new_leader, nodes)

    Process.sleep(300)

    history = ParallelClients.stop(clients)

    assert %{leader: ^new_leader} = wait_until(nexus, {Stability, :all})

    assert {:ok, _linearized_history, _ignored_ops} = Craft.Linearizability.linearize(history, Craft.SimpleMachine)
    # File.write!("history", :erlang.term_to_binary({linearized_history, ignored_ops}))
    # Craft.Linearizability.Visualization.to_file(nil)
  end

  defp random_command_fun(ctx) do
    fn i ->
      value =
        self()
        |> :erlang.pid_to_list()
        |> :erlang.list_to_binary()
        |> String.trim("<")
        |> String.trim(">")

      command =
        Enum.random([
          {:put, :a, "#{value}_#{i}"},
          {:get, :a}
        ])

      {command, Craft.command(command, ctx.name, ctx.nodes)}
    end
  end
end
