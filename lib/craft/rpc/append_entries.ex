defmodule Craft.RPC.AppendEntries do
  alias Craft.Log
  alias Craft.Log.Entry
  alias Craft.Consensus.LeaderState

  defstruct [
    :term,
    :leader_id,
    :prev_log_index,
    :prev_log_term,
    :entries,
    :leader_commit
  ]

  def new(%LeaderState{} = state, to_node) do
    next_index = Map.get(state.next_indices, to_node)
    prev_log_index = next_index - 1
    {:ok, %Entry{term: prev_log_term}} = Log.fetch(state.log, prev_log_index)
    entries = Log.fetch_from(state.log, next_index)

    %__MODULE__{
      term: state.current_term,
      leader_id: node(),
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: entries,
      leader_commit: 0
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

    def new(%FollowerState{} = state, success) do
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
