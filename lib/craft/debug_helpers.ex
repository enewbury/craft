defmodule Craft.DebugHelpers do
  @moduledoc """
  Helper functions for debugging Craft consensus issues
  """

  require Logger

  @doc """
  Monitor elections and quorum calculations for a group
  """
  def monitor_elections(group_name, interval_ms \\ 2000) do
    spawn(fn ->
      IO.inspect({:starting_monitor, group_name}, label: "ELECTION MONITOR")
      monitor_loop(group_name, interval_ms)
    end)
  end

  defp monitor_loop(group_name, interval_ms) do
    try do
      case Craft.state(group_name, node()) do
        {node_name, %{consensus: {consensus_state, _}}} ->
          state_name = consensus_state.state
          term = consensus_state.current_term
          leader = consensus_state.leader_id
          voting_nodes = MapSet.to_list(consensus_state.members.voting_nodes)
          quorum = Craft.Consensus.State.quorum_needed(consensus_state)

          election_info = if consensus_state.election do
            {:active_election, consensus_state.election.num_votes, MapSet.to_list(consensus_state.election.received_votes_from)}
          else
            :no_active_election
          end

          IO.inspect({:monitor_status, node_name, state_name, term, leader, voting_nodes, quorum, election_info},
                     label: "ELECTION MONITOR")

        error ->
          IO.inspect({:monitor_error, error}, label: "MONITOR ERROR")
      end
    catch
      :exit, reason ->
        IO.inspect({:monitor_exit, reason}, label: "MONITOR EXIT")
    rescue
      e ->
        IO.inspect({:monitor_exception, e}, label: "MONITOR EXCEPTION")
    end

    Process.sleep(interval_ms)
    monitor_loop(group_name, interval_ms)
  end

  @doc """
  Get current quorum info for debugging
  """
  def quorum_info(group_name) do
    case Craft.state(group_name, node()) do
      {node_name, %{consensus: {consensus_state, _}}} ->
        voting_nodes = MapSet.to_list(consensus_state.members.voting_nodes)
        num_voting = length(voting_nodes)
        quorum = Craft.Consensus.State.quorum_needed(consensus_state)

        %{
          node: node_name,
          state: consensus_state.state,
          term: consensus_state.current_term,
          leader: consensus_state.leader_id,
          voting_nodes: voting_nodes,
          num_voting: num_voting,
          quorum_needed: quorum,
          election: if(consensus_state.election, do: %{
            votes: consensus_state.election.num_votes,
            voters: MapSet.to_list(consensus_state.election.received_votes_from)
          }, else: nil)
        }
      error ->
        {:error, error}
    end
  end

  @doc """
  Check all nodes in cluster for quorum consistency
  """
  def check_cluster_quorum(group_name) do
    case Craft.state(group_name) do
      states when is_map(states) ->
        Enum.map(states, fn {node_name, %{consensus: consensus_result}} ->
          case consensus_result do
            {consensus_state, _} ->
              voting_nodes = MapSet.to_list(consensus_state.members.voting_nodes)
              quorum = Craft.Consensus.State.quorum_needed(consensus_state)

              {node_name, %{
                state: consensus_state.state,
                voting_nodes: voting_nodes,
                quorum_needed: quorum,
                leader: consensus_state.leader_id
              }}
            error ->
              {node_name, {:error, error}}
          end
        end)
      error ->
        {:error, error}
    end
  end
end
