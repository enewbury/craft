defmodule CraftTest do
  use ExUnit.Case
  alias Craft.Test.ClusterNodes
  alias Craft.TestHelper
  alias Craft.Log.MapLog
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState

  setup_all do
    nodes = ClusterNodes.spawn_nodes(5)

    [nodes: nodes]
  end

  test "greets the world", %{nodes: nodes} do
    states = [
      %FollowerState{log: Craft.Log.new(nil, MapLog)},
      %FollowerState{log: Craft.Log.new(nil, MapLog)},
      %FollowerState{log: Craft.Log.new(nil, MapLog)},
      %FollowerState{log: Craft.Log.new(nil, MapLog)},
      %CandidateState{log: Craft.Log.new(nil, MapLog)}
    ]

    TestHelper.start_group(states, nodes)

    receive do

    end
  end
end
