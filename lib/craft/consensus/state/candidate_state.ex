defmodule Craft.Consensus.CandidateState do
  alias Craft.Consensus
  alias Craft.RPC.RequestVote

  defstruct [
    :name,
    :other_nodes,
    {:current_term, -1},
    :log,
    :leader_id,

    :tracer_pid,

    num_votes: 1,
    received_votes_from: MapSet.new()
  ]

  def new(state) do
    %__MODULE__{
      name: state.name,
      other_nodes: state.other_nodes,
      current_term: state.current_term + 1,
      log: state.log,
      tracer_pid: state.tracer_pid,
      leader_id: state.leader_id
    }
  end

  def record_vote(%__MODULE__{} = state, %RequestVote.Results{} = results) do
    if not MapSet.member?(state.received_votes_from, results.from) do
      %__MODULE__{
        state |
        num_votes: state.num_votes + if(results.vote_granted, do: 1, else: 0),
        received_votes_from: MapSet.put(state.received_votes_from, results.from)
      }
    else
      state
    end
  end

  def won_election?(%__MODULE__{} = state) do
    Consensus.quorum_reached?(state, state.num_votes)
  end
end
