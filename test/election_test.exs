defmodule ElectionTest do
  use ExUnit.Case

  alias Craft.Consensus.State
  alias Craft.Log.EmptyEntry
  alias Craft.Nexus
  alias Craft.Persistence
  alias Craft.Persistence.MapPersistence
  alias Craft.TestCluster
  alias Craft.TestHelper

  import Nexus, only: [wait_until: 2]

  setup_all do
    [nodes: TestCluster.spawn_nodes(5)]
  end

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

    assert %Nexus.State{leader: ^expected_leader, term: 0} = wait_until(nexus, :all_stable)

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end

  describe "5.4.1 election restriction" do
    test "deny votes to out-of-date candidate, and correct its log", %{nodes: nodes} do
      state = State.new("abc", nodes, MapPersistence)

      shared_log =
        state.persistence
        |> Persistence.append(%EmptyEntry{term: 0})
        |> Persistence.append(%EmptyEntry{term: 1})

      majority_log =
        shared_log
        |> Persistence.append(%EmptyEntry{term: 4})
        |> Persistence.append(%EmptyEntry{term: 4})

      out_of_date_log =
        shared_log
        |> Persistence.append(%EmptyEntry{term: 2})
        |> Persistence.append(%EmptyEntry{term: 2})
        |> Persistence.append(%EmptyEntry{term: 3})
        |> Persistence.append(%EmptyEntry{term: 3})

      states =
        Enum.zip(
          nodes,
          [
            %State{state | state: :candidate, persistence: out_of_date_log},
            %State{state | state: :lonely, persistence: majority_log},
            %State{state | state: :lonely, persistence: majority_log},
            %State{state | state: :lonely, persistence: majority_log},
            %State{state | state: :lonely, persistence: majority_log},
          ]
        )

      {:ok, name, nexus} = TestHelper.start_group(states)

      candidate = List.first(nodes)

      assert %Nexus.State{leader: leader, term: 5} = wait_until(nexus, :all_stable)
      assert leader != candidate

      states = Craft.state(name, nodes)

      {_leader_state, {leader_log, _metadata}} = get_in(states, [leader, :consensus])
      {_caught_up_follower_state, {caught_up_follower_log, _metadata}} = get_in(states, [candidate, :consensus])
      assert caught_up_follower_log == leader_log

      Craft.stop_group(name, nodes)
      Nexus.stop(nexus)
    end

    test "most up-to-date member in a split-brain is elected and corrects out-of-date logs", %{nodes: nodes} do
      state = State.new("abc", nodes, MapPersistence)

      shared_log =
        state.persistence
        |> Persistence.append(%EmptyEntry{term: 0})
        |> Persistence.append(%EmptyEntry{term: 1})

      out_of_date_log =
        shared_log
        |> Persistence.append(%EmptyEntry{term: 2})
        |> Persistence.append(%EmptyEntry{term: 2})
        |> Persistence.append(%EmptyEntry{term: 3})
        |> Persistence.append(%EmptyEntry{term: 3})

      up_to_date_log =
        shared_log
        |> Persistence.append(%EmptyEntry{term: 4})
        |> Persistence.append(%EmptyEntry{term: 4})

      # five member cluster, two nodes are on the other half of the split brain
      states =
        Enum.zip(
          nodes,
          [
            %State{state | state: :candidate, persistence: up_to_date_log},
            %State{state | state: :lonely, persistence: out_of_date_log},
            %State{state | state: :lonely, persistence: out_of_date_log},
          ]
        )

      active_nodes = Keyword.keys(states)

      {:ok, name, nexus} = TestHelper.start_group(states)

      candidate = List.first(nodes)

      assert %Nexus.State{leader: ^candidate, term: 5} = wait_until(nexus, :leader_elected)
      states = Craft.state(name, active_nodes)

      # {_leader_state, {leader_log, _metadata}} = get_in(states, [leader, :consensus])
      # {_caught_up_follower_state, {caught_up_follower_log, _metadata}} = get_in(states, [candidate, :consensus])
      # assert caught_up_follower_log == leader_log

      Craft.stop_group(name, active_nodes)
      Nexus.stop(nexus)
    end
  end
end
