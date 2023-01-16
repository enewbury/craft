defmodule Craft.Consensus.LonelyFollowerState do
  alias Craft.Consensus.State
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.RPC.RequestVote

  def new(%State{} = state) do
    %State{
      state |
      mode_state: {
        %FollowerState{},
        %CandidateState{}
      }
    }
  end

  def vote(%State{mode_state: {follower_state, candidate_state}} = state, %RequestVote{} = request_vote) do
    {vote, follower_state} = FollowerState.vote(state, follower_state, request_vote)

    {vote, %State{state | mode_state: {follower_state, candidate_state}}}
  end

  def record_vote(%State{mode_state: {follower_state, candidate_state}} = state, %RequestVote.Results{} = results) do
    %State{
      state |
      mode_state: {follower_state, CandidateState.record_vote(candidate_state, results)}
    }
  end

  def election_result(%State{mode_state: {_follower_state, candidate_state}} = state) do
    CandidateState.election_result(state, candidate_state)
  end
end
