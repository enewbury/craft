defmodule Craft.Consensus.LeaderState do
  alias Craft.Consensus
  alias Craft.Consensus.State
  alias Craft.Consensus.State.Configuration
  alias Craft.Log
  alias Craft.Log.NewConfigurationEntry
  alias Craft.RPC.AppendEntries

  defstruct [
    :next_indices,
    :match_indices,
    :membership_change_request_from,
    client_requests: %{},
  ]

  def new(%State{} = state) do
    next_index = Log.latest_index(state.log) + 1
    next_indices = state |> State.other_nodes() |> Map.new(&{&1, next_index})
    match_indices = state |> State.other_nodes() |> Map.new(&{&1, 0})

    %State{
      state |
      leader_id: node(),
      mode_state: %__MODULE__{
        next_indices: next_indices,
        match_indices: match_indices,
      }
    }
  end

  def config_change_in_progress?(%State{} = state) do
    state.log
    |> Log.fetch_from(state.commit_index)
    |> Enum.any?(fn
      %NewConfigurationEntry{} -> true
      _ -> false
    end)
  end

  def add_node(%State{} = state, node) do
    non_voting_nodes = MapSet.put(state.configuration.non_voting_nodes, node)

    put_in(state.configuration.non_voting_nodes, non_voting_nodes)
  end

  def remove_node(%State{} = state, node) do
    voting_nodes = MapSet.delete(state.configuration.voting_nodes, node)
    non_voting_nodes = MapSet.delete(state.configuration.non_voting_nodes, node)

    %State{
      state |
      configuration:
        %Configuration{
          state.configuration|
          voting_nodes: voting_nodes,
          non_voting_nodes: non_voting_nodes,
        }
    }
  end

  def handle_append_entries_results(%State{} = state, %AppendEntries.Results{success: true} = results) do
    match_indices = Map.put(state.mode_state.match_indices, results.from, results.latest_index)
    next_indices = Map.put(state.mode_state.next_indices, results.from, results.latest_index + 1)
    state = %State{state | mode_state: %__MODULE__{state.mode_state | next_indices: next_indices, match_indices: match_indices}}

    # find the highest uncommitted match index shared by a majority of servers
    # this can be optimized to some degree (mapset, gb_tree, etc...)
    # also optimized by pre-computing quorum requirement and storing in state
    #
    # when we become leader, match indexes work their way up from zero non-uniformly
    # so it's entirely possible that we don't find a quorum of followers with a match index
    # until match indexes work their way up to parity
    highest_uncommitted_match_index =
      state.mode_state.match_indices
      |> Map.values()
      |> Enum.filter(fn index -> index >= state.commit_index end)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.reverse()
      |> Enum.find(fn index ->
        num_members_with_index = Enum.count(state.mode_state.match_indices, fn {_node, match_index} -> match_index >= index end)
        Consensus.quorum_reached?(state, num_members_with_index)
      end)

    with false <- is_nil(highest_uncommitted_match_index),
      {:ok, entry} <- Log.fetch(state.log, highest_uncommitted_match_index),
      true <- entry.term == state.current_term do
      %State{state | commit_index: highest_uncommitted_match_index}
    else
      _ ->
        state
    end
  end

  def handle_append_entries_results(%State{} = state, %AppendEntries.Results{success: false} = results) do
    next_indices = Map.update!(state.mode_state.next_indices, results.from, fn next_index -> next_index - 1 end)

    %State{state | mode_state: %__MODULE__{state.mode_state | next_indices: next_indices}}
  end
end
