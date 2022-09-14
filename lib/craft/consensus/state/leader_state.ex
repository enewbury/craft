defmodule Craft.Consensus.LeaderState do
  alias Craft.Consensus.CandidateState
  alias Craft.Log

  defstruct [
    :name,
    :other_nodes,
    :current_term,
    :log,
    :next_indices,
    :match_indices
  ]

  def new(%CandidateState{} = state) do
    next_index = Log.latest_index(state.log) + 1
    next_indices = Map.new(state.other_nodes, &{&1, next_index})
    match_indices = Map.new(state.other_nodes, &{&1, 0})

    %__MODULE__{
      name: state.name,
      other_nodes: state.other_nodes,
      current_term: state.current_term,
      log: state.log,
      next_indices: next_indices,
      match_indices: match_indices
    }
  end
end
