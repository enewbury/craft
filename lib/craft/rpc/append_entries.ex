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
    prev_log_index = Map.get(state.next_indices, to_node) - 1
    {:ok, %Entry{term: prev_log_term}} = Log.fetch(state.log, prev_log_index)

    %__MODULE__{
      term: state.current_term,
      leader_id: node(),
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      entries: [],
      leader_commit: 0
    }
  end

  defmodule Results do
    alias Craft.Consensus.FollowerState

    defstruct [
      :term,
      :from,
      :success
    ]

    def new(%FollowerState{} = state, success) do
      %__MODULE__{
        term: state.current_term,
        from: node(),
        success: success
      }
    end
  end

  # defmodule SuccessResult
  # defmodule FailureResult
end
