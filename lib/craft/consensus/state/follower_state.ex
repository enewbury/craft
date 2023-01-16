defmodule Craft.Consensus.FollowerState do
  alias Craft.Consensus.State
  alias Craft.Log
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.RequestVote

  defstruct [:voted_for]

  def new(%State{} = state) do
    %State{state | mode_state: %__MODULE__{}}
  end

  def vote_for?(%State{} = state, %RequestVote{} = request_vote) do
    request_vote.last_log_term > Log.latest_term(state.log) ||
    (
      request_vote.last_log_term == Log.latest_term(state.log) &&
        request_vote.last_log_index >= Log.latest_index(state.log)
    )
  end

  def vote(%State{} = state, %RequestVote{} = request_vote) do
    {vote, follower_state} = vote(state, state.mode_state, request_vote)

    {vote, %State{state | mode_state: follower_state}}
  end

  def vote(%State{} = state, %__MODULE__{voted_for: nil} = follower_state, %RequestVote{} = request_vote) do
    if vote_for?(state, request_vote) do
      {true, %__MODULE__{follower_state | voted_for: request_vote.candidate_id}}
    else
      {false, state}
    end
  end

  # repeat vote if asked
  def vote(%State{}, %__MODULE__{} = follower_state, %RequestVote{} = request_vote) do
    {follower_state.voted_for == request_vote.candidate_id, follower_state}
  end


  def append_entries(%State{current_term: current_term} = state, %AppendEntries{term: term}) when term < current_term, do: {false, state}

  #TODO: store latest log entry index/term in state so we can pattern match instead of querying the log module?
  # plenty of optimizations to be had here
  def append_entries(%State{} = state, %AppendEntries{} = append_entries) do
    state = %State{state | leader_id: append_entries.leader_id}

    if Log.latest_index(state.log) == append_entries.prev_log_index && Log.latest_term(state.log) == append_entries.prev_log_term do
      log = Log.append(state.log, append_entries.entries)

      state = %State{state | log: log, commit_index: min(append_entries.leader_commit, Log.latest_index(log))}

      {true, state}
    else
      {false, %State{state | log: Log.rewind(state.log, append_entries.prev_log_index)}}
    end
  end
end
