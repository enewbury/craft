defmodule CraftTest do
  use ExUnit.Case
  alias Craft.Test.ClusterNodes
  alias Craft.Log.MapLog
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.Nexus

  import Nexus, only: [wait_until: 2]

  setup_all do
    nodes = ClusterNodes.spawn_nodes(5)

    [nodes: nodes]
  end

  # describe "smoke tests" do
  #   test "starts a group, elects a leader, replicates logs, processes commands" do

  #   end
  # end

  test "pre-chosen candidate becomes leader", %{nodes: nodes} do
    log = Craft.Log.new(nil, MapLog)

    states =
      Enum.zip(
        nodes,
        [
          %CandidateState{log: log},
          %FollowerState{log: log},
          %FollowerState{log: log},
          %FollowerState{log: log},
          %FollowerState{log: log}
        ]
      )

    expected_leader = List.first(nodes)

    {:ok, nexus} = Nexus.start_link(states)

    assert %Nexus.State{leader: ^expected_leader, term: 0} = wait_until(nexus, :group_stable)
  end

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
