defmodule Craft.Consensus.FollowerState do
  alias Craft.Consensus.State
  alias Craft.Log
  alias Craft.Log.MembershipEntry
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.RequestVote

  require Logger

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


  def append_entries(%State{current_term: current_term} = state, %AppendEntries{term: term} = append_entries) when term < current_term do
    Logger.info("denying #{inspect append_entries} for earlier term #{term}", State.logger_metadata(state))

    {false, state}
  end

  #TODO: store latest log entry index/term in state so we can pattern match instead of querying the log module?
  # plenty of optimizations to be had here
  def append_entries(%State{} = state, %AppendEntries{prev_log_term: prev_log_term} = append_entries) do
    state = %State{state | leader_id: append_entries.leader_id}

    case Log.fetch(state.log, append_entries.prev_log_index) do
      {:ok, %{term: ^prev_log_term}} ->

        rewound_entries = Log.fetch_from(state.log, append_entries.prev_log_index + 1)

        log =
          state.log
          |> Log.rewind(append_entries.prev_log_index)
          |> Log.append(append_entries.entries)

        state = %State{state | log: log, commit_index: min(append_entries.leader_commit, Log.latest_index(log))}

        new_membership_entry =
          append_entries.entries
          |> Enum.reverse()
          |> Enum.find(fn
            %MembershipEntry{} ->
              true

            _ ->
              false
          end)

        case new_membership_entry do
          %MembershipEntry{members: members} ->
            {true, %State{state | members: members}}

          # if the entries that we've rewound contained a membership entry, and the incoming entries from the
          # leader don't include a new membership entry, we need to look back through the log until we find one
          # to determine the current cluster membership (section 4.1)
          nil ->
            Enum.find_value(rewound_entries, {true, state}, fn
              %MembershipEntry{members: members} ->
                {true, %State{state | members: members}}

              _ ->
                false
            end)
        end

      _ ->
        {false, %State{state | log: Log.rewind(state.log, append_entries.prev_log_index)}}
    end
  end
end
