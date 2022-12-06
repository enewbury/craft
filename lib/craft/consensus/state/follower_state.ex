defmodule Craft.Consensus.FollowerState do
  alias Craft.Log
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.RequestVote

  defstruct [
    :name,
    :other_nodes,
    {:current_term, -1},
    :log,

    :voted_for,
    :leader_id,

    :tracer_pid,

    commit_index: 0
  ]

  def new(state) do
    %__MODULE__{
      name: state.name,
      other_nodes: state.other_nodes,
      current_term: state.current_term,
      log: state.log,
      tracer_pid: state.tracer_pid,
      commit_index: state.commit_index
    }
  end

  # vote 'no' for lower term candidates
  def vote(%__MODULE__{current_term: current_term} = state, %RequestVote{term: term}) when term < current_term, do: {false, state}

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

  def append_entries(%__MODULE__{current_term: current_term} = state, %AppendEntries{term: term}) when term < current_term, do: {false, state}

  #TODO: store latest log entry index/term in state so we can pattern match instead of querying the log module?
  # plenty of optimizations to be had here
  def append_entries(%__MODULE__{} = state, %AppendEntries{} = append_entries) do
    state = %__MODULE__{state | leader_id: append_entries.leader_id}

    if Log.latest_index(state.log) == append_entries.prev_log_index && Log.latest_term(state.log) == append_entries.prev_log_term do
      log = Log.append(state.log, append_entries.entries)

      state = %__MODULE__{state | log: log, commit_index: min(append_entries.leader_commit, Log.latest_index(log))}

      {true, state}
    else
      {false, %__MODULE__{state | log: Log.rewind(state.log, append_entries.prev_log_index)}}
    end
  end
end
