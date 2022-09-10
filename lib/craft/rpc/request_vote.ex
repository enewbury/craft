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
    alias Craft.Consensus.CandidateState
    alias Craft.Consensus.FollowerState
    alias Craft.RPC.RequestVote

    defstruct [
      :term,
      :from,
      :vote_granted
    ]

    def new(%state_type{} = state, vote_granted) when state_type in [FollowerState, CandidateState] do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        vote_granted: vote_granted
      }
    end
  end
end
