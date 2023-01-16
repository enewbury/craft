defmodule Craft.Consensus.CandidateState do
  alias Craft.Consensus.State
  alias Craft.RPC.RequestVote

  defstruct [
    num_votes: 1,
    received_votes_from: MapSet.new(),
  ]

  def new(%State{} = state) do
    %State{
      state |
      current_term: state.current_term + 1,
      mode_state: %__MODULE__{}
    }
  end

  def record_vote(%State{} = state, %RequestVote.Results{} = results) do
    %State{state | mode_state: record_vote(state.mode_state, results)}
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

  def election_result(%State{} = state) do
    election_result(state, state.mode_state)
  end

  def election_result(%State{} = state, %__MODULE__{} = candidate_state) do
    num_members = length(state.other_nodes) + 1
    quorum_needed = div(num_members, 2) + 1

    num_voted_no = MapSet.size(candidate_state.received_votes_from) - candidate_state.num_votes

    cond do
      candidate_state.num_votes >= quorum_needed ->
        :won

      num_voted_no >= quorum_needed ->
        :lost

      true ->
        :pending
    end
  end
end
