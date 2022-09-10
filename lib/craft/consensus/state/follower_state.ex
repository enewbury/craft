defmodule Craft.Consensus.FollowerState do
  alias Craft.Log
  alias Craft.RPC.RequestVote
  alias Craft.RPC.AppendEntries

  defstruct [
    :name,
    :other_nodes,
    {:current_term, -1},
    :log,

    :voted_for,
    :leader_id
  ]

  def new(state) do
    %__MODULE__{
      name: state.name,
      other_nodes: state.other_nodes,
      current_term: state.current_term,
      log: state.log
    }
  end

  # vote 'no' for lower term candidates
  def vote(%__MODULE__{current_term: current_term} = state, %RequestVote{term: term} = request_vote) when term < current_term do
    {false, state}
  end

  # maybe vote for candidate in our term if we haven't voted for anyone else
  def vote(%__MODULE__{voted_for: nil, current_term: term} = state, %RequestVote{term: term} = request_vote) do
    their_log_more_up_to_date =
      request_vote.last_log_term > Log.latest_term(state.log) ||
      (
        request_vote.last_log_term == Log.latest_term(state.log) &&
        request_vote.last_log_index >= Log.latest_index(state.log)
      )

    if their_log_more_up_to_date do
      {true, %__MODULE__{state | voted_for: request_vote.candidate_id}}
    else
      {false, state}
    end
  end

  # repeat vote if asked
  def vote(%__MODULE__{} = state, %RequestVote{} = request_vote) do
    {state.voted_for == request_vote.candidate_id, state}
  end

  def append_entries(%__MODULE__{current_term: term} = state, %AppendEntries{term: term} = append_entries) do
    {true, %__MODULE__{state | leader_id: append_entries.leader_id}}
  end

  # if the message is from an older term
  def append_entries(%__MODULE__{} = state, %AppendEntries{}) do
    {false, state}
  end
end
