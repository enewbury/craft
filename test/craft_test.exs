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

  # describe "smoke tests" do
  #   test "starts a group, elects a leader, replicates logs, processes commands" do

  #   end
  # end

  test "greets the world", %{nodes: nodes} do
    log = Craft.Log.new(nil, MapLog)

    states = [
      %CandidateState{log: log, tracer_pid: self()},

      %FollowerState{log: log, tracer_pid: self()},
      %FollowerState{log: log, tracer_pid: self()},
      %FollowerState{log: log, tracer_pid: self()},
      %FollowerState{log: log, tracer_pid: self()}
    ]

    TestHelper.start_group(states, nodes)

        forward = fn(func) ->
          receive do
            msg ->
              IO.inspect msg
              func.(func)
          end
        end

        forward.(forward)
  end
end
