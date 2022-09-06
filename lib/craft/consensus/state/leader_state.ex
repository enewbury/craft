defmodule Craft.Consensus.LeaderState do
  defstruct [
    :name,
    :other_nodes,
    :current_term,
    :log
  ]

  def new(state) do
    %__MODULE__{
      name: state.name,
      other_nodes: state.other_nodes,
      current_term: state.current_term,
      log: state.log
    }
  end
end
