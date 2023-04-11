defmodule Craft.Consensus.LonelyState do
  alias Craft.Consensus
  alias Craft.Consensus.ElectionState
  alias Craft.Consensus.State
  alias Craft.RPC.RequestVote

  defstruct [
    :voted_for,
    :pre_vote_state
  ]

  def new(%State{} = state) do
    %State{state | mode_state: %__MODULE__{pre_vote_state: ElectionState.new(state)}}
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

  def record_pre_vote(%State{} = state, %RequestVote.Results{} = results) do
    put_in(state.mode_state.pre_vote_state, ElectionState.record_vote(state.mode_state.pre_vote_state, results))
  end

  def pre_vote_election_result(%State{mode_state: election_state} = state) do
    ElectionState.election_result(state, election_state.pre_vote_state)
  end
end
