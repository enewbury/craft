defmodule Craft.RPC.AppendEntries do
  alias Craft.Consensus.LeaderState

  defstruct [
    :term,
    :leader_id,
    :prev_log_index,
    :prev_log_term,
    :entries,
    :leader_commit
  ]

  def new(%LeaderState{current_term: term}, _to_node) do
    %__MODULE__{
      term: term,
      leader_id: node(),
      prev_log_index: nil,
      prev_log_term: nil,
      entries: [],
      leader_commit: nil
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
end
