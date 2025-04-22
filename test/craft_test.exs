defmodule CraftTest do
  use Craft.NexusCase

  alias Craft.Nexus.Stability
  alias Craft.SimpleMachine

  nexus_test "starts a group, elects a leader, replicates logs, processes commands", %{name: name, nexus: nexus} do
    wait_until(nexus, {Stability, :all})

    assert :ok = SimpleMachine.put(name, :a, 123)
    assert {:ok, 123} = SimpleMachine.get(name, :a)
  end

  nexus_test "leadership transfer", %{nodes: nodes, name: name, nexus: nexus} do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    new_leader = Enum.random(nodes -- [leader])
    Craft.transfer_leadership(name, new_leader)

    assert %{leader: ^new_leader} = wait_until(nexus, {Stability, :all})
  end
end
