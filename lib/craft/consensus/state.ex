defmodule Craft.Consensus.State do
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.LeaderState
  alias Craft.Log

  defstruct [
    :name,
    :members,
    :log,
    :nexus_pid,
    :leader_id,
    {:current_term, -1},
    {:commit_index, 0},

    :mode_state
  ]

  defmodule Members do
    defstruct [
      # non-voting query nodes or nodes that are new to the cluster that are still catching up
      :non_voting_nodes,
      :voting_nodes
    ]

    def new(voting_nodes, non_voting_members \\ []) do
      %__MODULE__{
        voting_nodes: MapSet.new(voting_nodes),
        non_voting_nodes: MapSet.new(non_voting_members)
      }
    end

    # members are initially non-voting while they catch up
    def add_member(%__MODULE__{} = members, node) do
      %__MODULE__{
        members |
        non_voting_nodes: MapSet.put(members.non_voting_nodes, node)
      }
    end

    def remove_member(%__MODULE__{} = members, node) do
      %__MODULE__{
        members |
        voting_nodes: MapSet.delete(members.voting_nodes, node),
        non_voting_nodes: MapSet.delete(members.non_voting_nodes, node)
      }
    end
  end

  def new(name, nodes, log_module) do
    %__MODULE__{
      name: name,
      members: Members.new(nodes),
      log: Log.new(name, log_module)
    }
  end

  # TODO: pre-compute quorum and cache
  def quorum_needed(%__MODULE__{} = state) do
    num_members = MapSet.size(state.members.voting_nodes) + 1

    div(num_members, 2) + 1
  end

  def other_voting_nodes(%__MODULE__{} = state) do
    MapSet.delete(state.members.voting_nodes, node())
  end

  # TODO: pre-compute and cache
  def other_nodes(%__MODULE__{} = state) do
    state.members.voting_nodes
    |> MapSet.union(state.members.non_voting_nodes)
    |> MapSet.delete(node())
  end

  def logger_metadata(%__MODULE__{} = state, extras \\ []) do
    # color =
    #   node()
    #   |> :erlang.phash2(255)
    #   |> IO.ANSI.color()

    color =
      case state.mode_state do
        %FollowerState{} ->
          :cyan

        {_, _} ->
          :light_red

        %CandidateState{} ->
          :blue

        %LeaderState{} ->
          :green
      end

    time =
      Time.utc_now()
      |> Time.to_string()

    # elixir uses the :time keyword, we want a higher resolution timestamp
    Keyword.merge([term: state.current_term, ansi_color: color, t: time], extras)
  end
end
