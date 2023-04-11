defmodule Craft.Consensus.CandidateState do
  alias Craft.Consensus.ElectionState
  alias Craft.Consensus.State
  alias Craft.RPC.RequestVote

  defstruct [
    :election_state,
    :leadership_transfer_request_id
  ]

  def new(%State{} = state, leadership_transfer_request_id \\ nil) do
    %State{
      state |
      current_term: state.current_term + 1,
      mode_state: %__MODULE__{
        leadership_transfer_request_id: leadership_transfer_request_id,
        election_state: ElectionState.new(state)
      }
    }
  end

  def record_vote(%State{} = state, %RequestVote.Results{} = results) do
    put_in(state.mode_state.election_state, ElectionState.record_vote(state.mode_state.election_state, results))
  end

  def election_result(%State{} = state) do
    ElectionState.election_result(state, state.mode_state.election_state)
  end
end
