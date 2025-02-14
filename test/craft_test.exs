defmodule CraftTest do
  use ExUnit.Case
  alias Craft.Consensus.State
  alias Craft.Persistence.MapPersistence
  alias Craft.Nexus

  # alias Craft.SimpleMachine
  alias Craft.TestCluster
  alias Craft.TestHelper

  import Nexus, only: [wait_until: 2]

  setup_all do
    [
      nodes: TestCluster.spawn_nodes(5) |> Enum.map(&Map.fetch!(&1, :node))
    ]
  end

  # describe "smoke tests" do
  #   test "starts a group, elects a leader, replicates logs, processes commands" do

  #   end
  # end

  test "pre-chosen candidate becomes leader", %{nodes: nodes} do
    state = State.new("abc", nodes, MapPersistence)

    states =
      Enum.zip(
        nodes,
        [
          %State{state | state: :candidate},
          %State{state | state: :lonely},
          %State{state | state: :lonely},
          %State{state | state: :lonely},
          %State{state | state: :lonely}
        ]
      )

    expected_leader = List.first(nodes)

    {:ok, name, nexus} = TestHelper.start_group(states)

    assert %Nexus.State{leader: ^expected_leader, term: 0} = wait_until(nexus, :group_stable)

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end

  # test "commands", %{nodes: nodes} do
  #   {:ok, name, nexus} = TestHelper.start_group(nodes)

  #   wait_until(nexus, :group_stable)

  #   assert :ok = SimpleMachine.put(name, nodes, :a, 123)
  #   assert {:ok, 123} = SimpleMachine.get(name, nodes, :a)

  #   Craft.stop_group(name, nodes)
  #   Nexus.stop(nexus)
  # end

  # test api design:
  #
  # series of:
  #   {until, nemesis}
  #
  #  ex:
  #  start in split brain, wait for stability (leader elected, etc), send a command to the group, then heal connectivity and wait for stability
  #  [{:group_stable, SplitBrain},
  #   fn group -> :ok = Craft.command(group, :some_command) end,
  #   {:group_stable, nil}]
end
