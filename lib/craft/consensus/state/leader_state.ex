defmodule Craft.Consensus.LeaderState do
  alias Craft.Consensus
  alias Craft.Log
  alias Craft.RPC.AppendEntries

  defstruct [
    :name,
    :other_nodes,
    :current_term,
    :log,
    :next_indices,
    :match_indices,
    :tracer_pid,
    commit_index: 0,
    client_requests: %{}
  ]

  def new(state) do
    next_index = Log.latest_index(state.log) + 1
    next_indices = Map.new(state.other_nodes, &{&1, next_index})
    match_indices = Map.new(state.other_nodes, &{&1, 0})

    %__MODULE__{
      name: state.name,
      other_nodes: state.other_nodes,
      current_term: state.current_term,
      log: state.log,
      next_indices: next_indices,
      match_indices: match_indices,
      tracer_pid: state.tracer_pid,
    }
  end

  def handle_append_entries_results(%__MODULE__{} = state, %AppendEntries.Results{success: true} = results) do
    match_indices = Map.put(state.match_indices, results.from, results.latest_index)
    next_indices = Map.put(state.next_indices, results.from, results.latest_index + 1)

    # Consensus.quorum_reached?(state, )

    %__MODULE__{state | next_indices: next_indices, match_indices: match_indices}
  end

  def handle_append_entries_results(%__MODULE__{} = state, %AppendEntries.Results{success: false} = results) do
    next_indices = Map.update!(state.next_indices, results.from, fn next_index -> next_index - 1 end)

    %__MODULE__{state | next_indices: next_indices}
  end
end
