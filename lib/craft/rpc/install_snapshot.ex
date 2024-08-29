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

  defmodule Results do
    defstruct [
      # if the leader is deposed during a snapshot transfer, we still want to hear
      # about the results, so we can delete the snapshot if no other nodes are receiving it
      # so we don't include the term here
      :from,
      :success
    ]

    def new(success) do
      %__MODULE__{from: node(), success: success}
    end
  end
end
