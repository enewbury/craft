defmodule Craft.RPC.AppendEntries do
  alias Craft.Consensus.State

  defstruct [
    :term,
    :leader_id,
    :prev_log_index,
    :prev_log_term,
    :entries,
    :leader_commit
  ]

  def new(%State{current_term: term}, _to_node) do
    %__MODULE__{
      term: term,
      leader_id: node(),
      prev_log_index: nil,
      prev_log_term: nil,
      entries: [],
      leader_commit: nil
    }
  end
end
