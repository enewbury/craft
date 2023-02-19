defmodule Craft.RPC.AppendEntries do
  alias Craft.Consensus.State
  alias Craft.Consensus.LeaderState
  alias Craft.Log

  defstruct [
    :term,
    :leader_id,
    :prev_log_index,
    :prev_log_term,
    :entries,
    :leader_commit
  ]

  def new(%State{mode_state: %LeaderState{}} = state, to_node) do
    next_index = Map.get(state.mode_state.next_indices, to_node)
    prev_log_index = next_index - 1
    {:ok, %{term: prev_log_term}} = Log.fetch(state.log, prev_log_index)
    entries = Log.fetch_from(state.log, next_index)

    %__MODULE__{
      term: state.current_term,
      leader_id: node(),
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: entries,
      leader_commit: state.commit_index
    }
  end

  defmodule Results do
    alias Craft.Consensus.FollowerState

    defstruct [
      :term,
      :from,
      :success,
      :latest_index # if successful, the follower's latest index
    ]

    def new(%State{mode_state: %FollowerState{}} = state, success) do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        success: success,
        latest_index: Log.latest_index(state.log)
      }
    end
  end

  # defmodule SuccessResult
  # defmodule FailureResult
end
