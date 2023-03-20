defmodule Craft.RPC.RequestVote do
  alias Craft.Consensus.State

  defstruct [
    :term,
    :candidate_id,
    :last_log_index,
    :last_log_term,
    :pre_vote,
    # :leadership_transfer # section 4.2.3
  ]

  def new(%State{} = state, pre_vote: pre_vote) do
    %__MODULE__{
      term: state.current_term,
      candidate_id: node(),
      pre_vote: pre_vote
    }
  end

  defmodule Results do
    defstruct [
      :term,
      :from,
      :vote_granted,
      :pre_vote
    ]

    def new(%State{} = state, pre_vote, vote_granted) do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        vote_granted: vote_granted,
        pre_vote: pre_vote
      }
    end
  end
end
