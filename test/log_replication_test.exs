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

  test "leader rewinds follower logs, and fast-forwards them up to its own", %{nodes: nodes} do
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

    leader = List.first(nodes)
    stray_follower = List.last(nodes)
    assert %Nexus.State{leader: ^leader, term: 5} = wait_until(nexus, :group_stable)

    states = Craft.state(name, nodes)

    {:leader, leader_state} = get_in(states, [leader, :consensus])
    {:follower, stray_follower_state} = get_in(states, [stray_follower, :consensus])

    assert stray_follower_state.log == leader_state.log

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end
end
