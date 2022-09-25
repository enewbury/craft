defmodule Craft.Consensus.CandidateState do
  alias Craft.RPC.RequestVote

  defstruct [
    :name,
    :other_nodes,
    :log,
    :tracer_pid,
    current_term: -1,


    num_votes: 1,
    received_votes_from: MapSet.new()
  ]

  def new(state) do
    %__MODULE__{
      name: state.name,
      other_nodes: state.other_nodes,
      current_term: state.current_term + 1,
      log: state.log
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
    num_members = length(state.other_nodes) + 1
    quorum_needed = div(num_members, 2) + 1

    state.num_votes >= quorum_needed
  end
end
