defmodule Craft.ElectionTest do
  use Craft.NexusCase

  alias Craft.Consensus.State
  alias Craft.Log.EmptyEntry
  alias Craft.Nexus.Stability
  alias Craft.Persistence
  alias Craft.Persistence.MapPersistence
  alias Craft.TestGroup

  @tag :unmanaged
  nexus_test "pre-chosen candidate becomes leader", %{nodes: nodes} do
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

    {:ok, name, nexus} = TestGroup.start_group(states)

    assert %{leader: ^expected_leader, term: 0} = wait_until(nexus, {Stability, :all})

    Craft.stop_group(name)
  end

  describe "5.4.1 election restriction" do
    @tag :unmanaged
    nexus_test "deny votes to out-of-date candidate, and correct its log", %{nodes: nodes} do
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

      {:ok, name, nexus} = TestGroup.start_group(states)

      candidate = List.first(nodes)

      assert %{leader: leader, term: 5} = wait_until(nexus, {Stability, :all})
      assert leader != candidate

      states = Craft.state(name)

      {_leader_state, {leader_log, _metadata}} = get_in(states, [leader, :consensus])
      {_caught_up_follower_state, {caught_up_follower_log, _metadata}} = get_in(states, [candidate, :consensus])
      assert caught_up_follower_log == leader_log

      Craft.stop_group(name)
    end

    @tag :unmanaged
    nexus_test "most up-to-date member in a split-brain is elected and corrects out-of-date logs", %{nodes: nodes} do
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

      # five member cluster, two nodes are out of contact on the other half of the split brain
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

      {:ok, name, nexus} = TestGroup.start_group(states)

      up_to_date_node = List.first(nodes)

      assert %{leader: ^up_to_date_node, term: 5} = wait_until(nexus, {Stability, :majority})
      states = Map.new(active_nodes, &Craft.state(name, &1))

      {_leader_state, {leader_log, _metadata}} = get_in(states, [up_to_date_node, :consensus])
      for node <- active_nodes -- [up_to_date_node] do
        {_follower_state, {follower_log, _metadata}} = get_in(states, [node, :consensus])

        assert follower_log == leader_log
      end

      Craft.stop_group(name)
    end
  end
end
