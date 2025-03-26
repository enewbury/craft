defmodule CraftTest do
  use Craft.NexusCase

  alias Craft.Nexus.Stability
  alias Craft.SimpleMachine

  nexus_test "starts a group, elects a leader, replicates logs, processes commands", %{nodes: nodes, name: name, nexus: nexus} do
    wait_until(nexus, {Stability, :all})

    assert :ok = SimpleMachine.put(name, nodes, :a, 123)
    assert {:ok, 123} = SimpleMachine.get(name, nodes, :a)
  end

  nexus_test "leadership transfer", %{nodes: nodes, name: name, nexus: nexus} do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    new_leader = Enum.random(nodes -- [leader])
    Craft.transfer_leadership(name, new_leader, nodes)

    assert %{leader: ^new_leader} = wait_until(nexus, {Stability, :all})
  end
end
