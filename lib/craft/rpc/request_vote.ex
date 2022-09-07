defmodule Craft.RPC.RequestVote do
  alias Craft.Consensus.CandidateState

  defstruct [
    :term,
    :candidate_id,
    :last_log_index,
    :last_log_term
  ]

  def new(%CandidateState{current_term: term}) do
    %__MODULE__{
      term: term,
      candidate_id: node()
    }
  end

  defmodule Results do
    alias Craft.Consensus.FollowerState
    alias Craft.RPC.RequestVote

    defstruct [
      :term,
      :candidate_id,
      :vote_granted
    ]

    def new(%RequestVote{} = request_vote, %FollowerState{} = state) do
      do_new(state, state.voted_for == request_vote.candidate_id)
    end

    # candidates will always vote 'no' for other candidates
    def new(%RequestVote{}, %CandidateState{} = state) do
      do_new(state, false)
    end

    defp do_new(state, vote_granted) do
      %__MODULE__{
        term: state.current_term,
        candidate_id: node(),
        vote_granted: vote_granted
      }
    end
  end
end
