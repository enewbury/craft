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
    :nexus_pid,
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
      nexus_pid: state.nexus_pid,
      commit_index: state.commit_index
    }
  end

  def handle_append_entries_results(%__MODULE__{} = state, %AppendEntries.Results{success: true} = results) do
    match_indices = Map.put(state.match_indices, results.from, results.latest_index)
    next_indices = Map.put(state.next_indices, results.from, results.latest_index + 1)
    state = %__MODULE__{state | next_indices: next_indices, match_indices: match_indices}

    # find the highest uncommitted match index shared by a majority of servers
    # this can be optimized to some degree (mapset, gb_tree, etc...)
    # also optimized by pre-computing quorum requirement and storing in state
    highest_uncommitted_match_index =
      state.match_indices
      |> Map.values()
      |> Enum.filter(fn index -> index >= state.commit_index end)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.reverse()
      |> Enum.find(fn index ->
        num_members_with_index = Enum.count(state.match_indices, fn {_node, match_index} -> match_index >= index end)
        Consensus.quorum_reached?(state, num_members_with_index)
      end)

    {:ok, entry} = Log.fetch(state.log, highest_uncommitted_match_index)
    if entry.term == state.current_term do
      %__MODULE__{state | commit_index: highest_uncommitted_match_index}
    else
      state
    end
  end

  def handle_append_entries_results(%__MODULE__{} = state, %AppendEntries.Results{success: false} = results) do
    next_indices = Map.update!(state.next_indices, results.from, fn next_index -> next_index - 1 end)

    %__MODULE__{state | next_indices: next_indices}
  end
end
