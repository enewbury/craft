defmodule Craft.RPC.InstallSnapshot do
  defstruct [
    :term,
    :leader_id,
    :snapshot_transfer
  ]

  def new(state, snapshot_transfer) do
    %__MODULE__{
      term: state.current_term,
      leader_id: node(),
      snapshot_transfer: snapshot_transfer
    }
  end
end
