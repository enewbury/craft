defmodule Craft.Consensus.State do
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.LeaderState
  alias Craft.Consensus.LonelyState
  alias Craft.Consensus.FollowerState
  alias Craft.Persistence

  defstruct [
    :name,
    :members,
    :persistence,
    :nexus_pid,
    :leader_id,
    {:current_term, -1},
    {:commit_index, 0},

    :mode_state
  ]

  defmodule Members do
    defstruct [
      :catching_up_nodes,
      :non_voting_nodes,
      :voting_nodes
    ]

    def new(voting_nodes, non_voting_members \\ []) do
      %__MODULE__{
        catching_up_nodes: MapSet.new(),
        voting_nodes: MapSet.new(voting_nodes),
        non_voting_nodes: MapSet.new(non_voting_members)
      }
    end

    def add_member(%__MODULE__{} = members, node) do
      if MapSet.member?(members.voting_nodes, node) or MapSet.member?(members.non_voting_nodes, node) do
        raise "member already added"
      end

      %__MODULE__{
        members |
        catching_up_nodes: MapSet.put(members.catching_up_nodes, node)
      }
    end

    def remove_member(%__MODULE__{} = members, node) do
      %__MODULE__{
        members |
        catching_up_nodes: MapSet.delete(members.catching_up_nodes, node),
        voting_nodes: MapSet.delete(members.voting_nodes, node),
        non_voting_nodes: MapSet.delete(members.non_voting_nodes, node)
      }
    end

    def allow_node_to_vote(%__MODULE__{} = members, node) do
      %__MODULE__{
        members |
        voting_nodes: MapSet.put(members.voting_nodes, node),
        catching_up_nodes: MapSet.delete(members.catching_up_nodes, node),
        non_voting_nodes: MapSet.delete(members.non_voting_nodes, node)
      }
    end

    def can_vote?(%__MODULE__{} = members, member) do
      MapSet.member?(members.voting_nodes, member)
    end

    def this_node_can_vote?(%__MODULE__{} = members) do
      can_vote?(members, node())
    end
  end

  def new(name, nodes, persistence) do
    %__MODULE__{
      name: name,
      members: Members.new(nodes),
      persistence: Persistence.new(name, persistence)
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
    |> MapSet.union(state.members.catching_up_nodes)
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
        %LonelyState{} ->
          :light_red

        %FollowerState{} ->
          :cyan

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

  def set_current_term(%__MODULE__{} = state, term) do
    persistence = Persistence.put_current_term!(state.persistence, term)

    %__MODULE__{state | persistence: persistence, current_term: term}
  end

  def set_voted_for(%__MODULE__{} = state, voted_for) do
    persistence = Persistence.put_voted_for!(state.persistence, voted_for)
    mode_state = %{state.mode_state | voted_for: voted_for}

    %__MODULE__{state | persistence: persistence, mode_state: mode_state}
  end

  def restore(%__MODULE__{mode_state: %LonelyState{}} = state) do
    voted_for = Persistence.get_voted_for!(state.persistence)
    current_term = Persistence.get_current_term!(state.persistence)

    mode_state = %LonelyState{state.mode_state | voted_for: voted_for}

    %__MODULE__{state | mode_state: mode_state, current_term: current_term}
  end
end
