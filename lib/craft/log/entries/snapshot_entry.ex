defmodule Craft.Log.SnapshotEntry do
  alias Craft.Consensus.State

  defstruct [:term, :prev_entry_term, :members]

  def new(%State{} = state, term) do
    %__MODULE__{
      term: term,
      members: state.members
    }
  end
end
