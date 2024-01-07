defmodule Craft.Log.SnapshotEntry do
  alias Craft.Consensus.State
  #
  # this type of entry is never replicated to other members, it's just a stub prepended
  # to the log after it's been truncated post-snapshot
  #
  defstruct [:term, :members]

  def new(%State{} = state, term) do
    %__MODULE__{
      term: term,
      members: state.members
    }
  end
end
