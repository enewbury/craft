defmodule Craft.Consensus.FollowerState do
  alias Craft.Consensus
  alias Craft.Consensus.State
  alias Craft.RPC.RequestVote

  # followers only ever vote during a leadership transfer
  defstruct [:voted_for]

  def new(%State{} = state) do
    %State{state | mode_state: %__MODULE__{}}
  end

  def vote(%State{mode_state: %__MODULE__{voted_for: nil}} = state, %RequestVote{} = request_vote) do
    if Consensus.vote_for?(state, request_vote) do
      {true, put_in(state.mode_state.voted_for, request_vote.candidate_id)}
    else
      {false, state}
    end
  end

  # repeat vote if asked
  def vote(%State{} = state, %RequestVote{} = request_vote) do
    {state.mode_state.voted_for == request_vote.candidate_id, state}
  end
end
