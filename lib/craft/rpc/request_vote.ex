defmodule Craft.RPC.RequestVote do
  alias Craft.Consensus.State

  defstruct [
    :term,
    :candidate_id,
    :last_log_index,
    :last_log_term
  ]

  def new(%State{current_term: term}) do
    %__MODULE__{
      term: term,
      candidate_id: node()
    }
  end

  defmodule Results do
    defstruct [
      :term,
      :candidate_id,
      # instead of a boolean "vote_granted", we just say who we voted for
      :voted_for
    ]

    def new(%State{current_term: term, voted_for: voted_for}) do
      %__MODULE__{
        term: term,
        candidate_id: node(),
        voted_for: voted_for
      }
    end
  end
end
