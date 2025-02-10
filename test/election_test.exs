defmodule ElectionTest do
  use ExUnit.Case
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.State
  alias Craft.Log
  alias Craft.Log.MapLog
  alias Craft.Nexus

  alias Craft.SimpleMachine
  alias Craft.TestCluster
  alias Craft.TestHelper

  import Nexus, only: [wait_until: 2]

  setup_all do
    [nodes: TestCluster.spawn_nodes(5)]
  end

  test "pre-chosen candidate becomes leader", %{nodes: nodes} do
    log = Craft.Log.new(nil, MapLog)

    states =
      Enum.zip(
        nodes,
        [
          CandidateState.new(%State{log: log}),
          FollowerState.new(%State{log: log}),
          FollowerState.new(%State{log: log}),
          FollowerState.new(%State{log: log}),
          FollowerState.new(%State{log: log})
        ]
      )

    expected_leader = List.first(nodes)

    {:ok, name, nexus} = TestHelper.start_group(states)

    assert %Nexus.State{leader: ^expected_leader, term: 0} = wait_until(nexus, :group_stable)

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end

  test "5.4.1 election restriction", %{nodes: nodes} do
    shared_log =
      Log.new(nil, Log.MapLog)
      |> Log.append(%Log.CommandEntry{term: 0})
      |> Log.append(%Log.CommandEntry{term: 1})

    leader_log =
      shared_log
      |> Log.append(%Log.CommandEntry{term: 4, command: {:put, :a, 1}})
      |> Log.append(%Log.CommandEntry{term: 4, command: {:put, :b, 2}})

    stray_follower_log =
      shared_log
      |> Log.append(%Log.CommandEntry{term: 2, command: {:put, :a, 1}})
      |> Log.append(%Log.CommandEntry{term: 2, command: {:put, :b, 1}})
      |> Log.append(%Log.CommandEntry{term: 3, command: {:put, :c, 1}})
      |> Log.append(%Log.CommandEntry{term: 3, command: {:put, :d, 1}})

    states =
      Enum.zip(
        nodes,
        [
          CandidateState.new(%State{log: leader_log}),
          FollowerState.new(%State{log: leader_log}),
          FollowerState.new(%State{log: leader_log}),
          FollowerState.new(%State{log: leader_log}),

          FollowerState.new(%State{log: stray_follower_log})
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
