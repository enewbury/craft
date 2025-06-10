defmodule Craft.Log.SnapshotEntry do
  alias Craft.Consensus.State

  defstruct [
    :term,
    :members,
    :machine_private # contains private machine state for log-stored snapshot, nil otherwise
  ]

  def new(%State{} = state, term, path_or_content) do
    entry =
      %__MODULE__{
        term: term,
        members: state.members
      }

    if state.machine.__craft_mutable__() do
      entry
    else
      %{entry | machine_private: path_or_content}
    end
  end
end
