defmodule LogReplicationTest do
  use ExUnit.Case
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.FollowerState
  alias Craft.Log
  alias Craft.Nexus

  alias Craft.SimpleMachine
  alias Craft.Test.ClusterNodes
  alias Craft.TestHelper

  import Nexus, only: [wait_until: 2]

  setup_all do
    [nodes: ClusterNodes.spawn_nodes(5)]
  end

  test "leader rewinds follower logs", %{nodes: nodes} do
    shared_log =
      Log.new(nil, Log.MapLog)
      |> Log.append(%Log.Entry{term: 0})
      |> Log.append(%Log.Entry{term: 1})

    leader_log =
      shared_log
      |> Log.append(%Log.Entry{term: 4, command: {:put, :a, 1}})
      |> Log.append(%Log.Entry{term: 4, command: {:put, :b, 2}})

    stray_follower_log =
      shared_log
      |> Log.append(%Log.Entry{term: 2, command: {:put, :a, 1}})
      |> Log.append(%Log.Entry{term: 2, command: {:put, :b, 1}})
      |> Log.append(%Log.Entry{term: 3, command: {:put, :c, 1}})
      |> Log.append(%Log.Entry{term: 3, command: {:put, :d, 1}})

    states =
      Enum.zip(
        nodes,
        [
          %CandidateState{log: leader_log},
          %FollowerState{log: leader_log},
          %FollowerState{log: leader_log},
          %FollowerState{log: leader_log},

          %FollowerState{log: stray_follower_log}
        ]
      )

    {:ok, name, nexus} = TestHelper.start_group(states)

    expected_leader = List.first(nodes)
    assert %Nexus.State{leader: ^expected_leader, term: 5} = wait_until(nexus, :group_stable)

    Craft.state(name, nodes)
    |> IO.inspect

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end
end
