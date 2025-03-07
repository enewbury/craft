defmodule Craft.LivenessTests do
  use ExUnit.Case

  alias Craft.Nexus
  alias Craft.Nexus.Stability
  alias Craft.TestCluster
  alias Craft.TestHelper

  import Nexus, only: [wait_until: 2]

  setup_all do
    [nodes: TestCluster.spawn_nodes(5)]
  end

  test "leader without majority connectivity will step down (CheckQuorum)", %{nodes: nodes} do
    {:ok, name, nexus} = TestHelper.start_group(nodes)

    %Nexus.State{leader: leader} = wait_until(nexus, {Stability, :all})

    # majority =
    #   Enum.take(
    #     nodes -- [leader],
    #     div(Enum.count(nodes), 2) + 1
    #   )

    # Nexus.set_nemesis(nexus, fn {:cast, to, from, _msg}, state ->
    #   if from == leader and to in majority or from in majority and to == leader do
    #     {:drop, state}
    #   else
    #     {:forward, state}
    #   end
    # end)

    # %Nexus.State{leader: new_leader} = wait_until(nexus, :majority_stable)

    # assert new_leader != leader

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end

  # test "candidates isolated from the leader don't trigger needlessly disruptive and hopeless elections (PreVote)", %{nodes: nodes} do
  #   {:ok, name, nexus} = TestHelper.start_group(nodes)

  #   # %Nexus.State{leader: leader, term: term} = wait_until(nexus, :all_stable)

  #   node_isolated_from_leader = Enum.random(nodes -- [leader])

  #   Nexus.set_nemesis_and_wait_until(nexus, nil, fn {:cast, to, from, _msg}, state ->
  #     if from == leader and to == node_isolated_from_leader or from == node_isolated_from_leader and to == leader do
  #       {:drop, :cont, state}
  #     else
  #       {:forward, :cont, state}
  #     end
  #   end)

  #   # assert %Nexus.State{leader: ^leader, term: ^term} = wait_until(nexus, :majority_stable)

  #   Craft.stop_group(name, nodes)
  #   Nexus.stop(nexus)
  # end
end

