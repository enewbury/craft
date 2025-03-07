defmodule Craft.LivenessTests do
  use ExUnit.Case

  alias Craft.Nexus
  alias Craft.Nexus.Stability
  alias Craft.TestCluster
  alias Craft.TestHelper
  alias Craft.RPC.RequestVote

  import Nexus, only: [wait_until: 2, nemesis: 2, nemesis_and_wait_until: 3]

  setup_all do
    [nodes: TestCluster.spawn_nodes(5)]
  end

  test "leader without majority connectivity will step down (CheckQuorum)", %{nodes: nodes} do
    {:ok, name, nexus} = TestHelper.start_group(nodes)

    %{leader: leader} = wait_until(nexus, {Stability, :all})

    majority =
      Enum.take(
        nodes -- [leader],
        div(Enum.count(nodes), 2) + 1
      )

    nemesis(nexus, fn {:cast, to, from, _msg}, state ->
      if from == leader and to in majority or from in majority and to == leader do
        {:drop, state}
      else
        {:forward, state}
      end
    end)

    %{leader: new_leader} = wait_until(nexus, {Stability, :majority})

    assert new_leader != leader

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end

  test "nodes isolated from the leader don't trigger needlessly disruptive and hopeless elections (PreVote)", %{nodes: nodes} do
    {:ok, name, nexus} = TestHelper.start_group(nodes)

    %{leader: leader, term: term} = wait_until(nexus, {Stability, :all})

    node_isolated_from_leader = Enum.random(nodes -- [leader])

    nemesis_and_wait_until(
      nexus,
      fn {:cast, to, from, _msg}, state ->
        if from == leader and to == node_isolated_from_leader or from == node_isolated_from_leader and to == leader do
          {:drop, state}
        else
          {:forward, state}
        end
      end,
      fn
        {:cast, ^node_isolated_from_leader, from, %RequestVote.Results{pre_vote: true, vote_granted: false}}, awaiting_prevote_denials ->
          awaiting_prevote_denials = awaiting_prevote_denials || MapSet.new(nodes -- [leader, node_isolated_from_leader])
          awaiting_prevote_denials = MapSet.delete(awaiting_prevote_denials, from)

          if Enum.empty?(awaiting_prevote_denials) do
            :halt
          else
            {:cont, awaiting_prevote_denials}
          end

        _, state ->
          {:cont, state}
      end
    )

    %{leader: ^leader, term: ^term} = wait_until(nexus, {Stability, :majority})

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end
end

