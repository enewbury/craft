defmodule Craft.Consensus.State do
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.LeaderState

  defstruct [
    :name,
    :configuration,
    :log,
    :nexus_pid,
    :leader_id,
    {:current_term, -1},
    {:commit_index, 0},

    :mode_state
  ]

  defmodule Configuration do
    defstruct [
      :voting_nodes,
      # non-voting query replicas or nodes that are new to the cluster that are
      # still catching up
      :non_voting_nodes,

      # the application current state machine, will be used for machine upgrades later
      # :machine
    ]

    def new(voting_nodes, non_voting_nodes \\ []) do
      %__MODULE__{
        voting_nodes: MapSet.new(voting_nodes),
        non_voting_nodes: MapSet.new(non_voting_nodes)
      }
    end
  end

  # TODO: pre-compute quorum and cache
  def quorum_needed(%__MODULE__{} = state) do
    num_members = MapSet.size(state.configuration.voting_nodes) + 1

    div(num_members, 2) + 1
  end

  def other_voting_nodes(%__MODULE__{} = state) do
    MapSet.delete(state.configuration.voting_nodes, node())
  end

  # TODO: pre-compute and cache
  def other_nodes(%__MODULE__{} = state) do
    state.configuration.voting_nodes
    |> MapSet.union(state.configuration.non_voting_nodes)
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
