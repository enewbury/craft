defmodule Craft.Consensus.State do
  alias Craft.Configuration
  alias Craft.Consensus.State.Election
  alias Craft.Consensus.State.LeaderState
  alias Craft.Consensus.State.Members
  alias Craft.Log.MembershipEntry
  alias Craft.Log.SnapshotEntry
  alias Craft.Persistence
  alias Craft.Persistence.Metadata
  alias Craft.RPC.RequestVote

  require Logger

  defstruct [
    :state,
    :name,
    :members,
    :persistence,
    :machine,
    :nexus_pid,
    :leader_id,
    :global_clock,
    :lease_expires_at,
    {:snapshots, %{}},
    {:current_term, -1},
    {:commit_index, 0},

    :leader_state,
    :voted_for, # lonely and follower
    :election, # lonely and candidate
    :leadership_transfer_request_id, # candidate only
    :incoming_snapshot_transfer # receiving_snapshot only
  ]

  def new(name, nodes, persistence, machine, global_clock) do
    persistence = Persistence.new(name, persistence)

    # if we're restoring state from disk, search the log backwards for group members
    members =
      if nodes do
        Members.new(nodes)
      else
        entry =
          Persistence.reverse_find(persistence, fn
            %MembershipEntry{} -> true
            %SnapshotEntry{} -> true
            _ -> false
          end)

        # fall back to the nodes that the cluster was originally initialized with, the leader should contact us soon.
        if entry do
          entry.members
        else
          %{nodes: nodes} = Configuration.find(name)

          Members.new(nodes)
        end
      end

    state =
      %__MODULE__{
        name: name,
        members: members,
        persistence: persistence,
        machine: machine,
        global_clock: global_clock
      }

    case Metadata.fetch(persistence) do
      {:ok, %Metadata{} = metadata} ->
        %{
          state |
          current_term: metadata.current_term,
          voted_for: metadata.voted_for,
          lease_expires_at: metadata.lease_expires_at
        }

      :error ->
        Metadata.init(state)
    end
  end

  def set_current_term(%__MODULE__{} = state, new_current_term, voted_for \\ nil) do
    state =
      %{
        state |
        current_term: new_current_term,
        voted_for: voted_for
      }

    Metadata.update(state)

    state
  end

  def set_lease_expires_at(%__MODULE__{} = state, lease_expires_at) do
    state = %{state | lease_expires_at: lease_expires_at}

    Metadata.update(state)

    state
  end

  def become_lonely(%__MODULE__{} = state, new_current_term) do
    state
    |> become_lonely()
    |> set_current_term(new_current_term)
  end

  def become_lonely(%__MODULE__{} = state) do
    %{
      state |
      state: :lonely,
      leader_state: nil,
      leader_id: nil,
      leadership_transfer_request_id: nil,
      election: Election.new(state.members)
    }
  end

  def become_follower(%__MODULE__{} = state) do
    %{
      state |
      state: :follower,
      leader_state: nil,
      leadership_transfer_request_id: nil,
      election: nil
    }
  end

  def become_receiving_snapshot(%__MODULE__{} = state) do
    %{
      state |
      state: :receiving_snapshot,
      leader_state: nil,
      leadership_transfer_request_id: nil,
      election: nil
    }
  end

  # leadership_transfer_request_id set directly in Consensus
  # this makes test tracing easier as we don't need to special-case a tuple as `data` in Consensus.Tracer
  def become_candidate(%__MODULE__{} = state) do
    %{
      state |
      state: :candidate,
      leader_state: nil,
      election: Election.new(state.members),
    }
    |> set_current_term(state.current_term + 1, node())
  end

  def become_leader(%__MODULE__{} = state) do
    %{
      state |
      state: :leader,
      leader_id: node(),
      leader_state: LeaderState.new(state),
      election: nil
    }
    |> set_current_term(state.current_term)
  end

  # voting for others

  def vote_for?(%__MODULE__{current_term: current_term}, %RequestVote{term: term}) when term < current_term, do: false

  def vote_for?(%__MODULE__{} = state, %RequestVote{} = request_vote) do
    request_vote.last_log_term > Persistence.latest_term(state.persistence) ||
    (request_vote.last_log_term == Persistence.latest_term(state.persistence) &&
      request_vote.last_log_index >= Persistence.latest_index(state.persistence))
  end

  # holding pre-vote and leadership elections

  def record_vote(%__MODULE__{} = state, %RequestVote.Results{} = results) do
    %{state | election: Election.record_vote(state.election, results)}
  end

  def election_result(%__MODULE__{} = state) do
    Election.election_result(state.election, quorum_needed(state))
  end

  def quorum_needed(%__MODULE__{} = state) do
    num_members = MapSet.size(state.members.voting_nodes)

    div(num_members, 2) + 1
  end

  def snapshot_ready(%__MODULE__{} = state, index, path_or_content) do
    {:ok, %{term: term}} = Persistence.fetch(state.persistence, index)
    persistence = Persistence.truncate(state.persistence, index, SnapshotEntry.new(state, term, path_or_content))

    state =
      if state.leader_state do
        put_in(state.leader_state.snapshot_transfers, %{})
      else
        state
      end

    %{state | snapshots: Map.put(state.snapshots, index, path_or_content), persistence: persistence}
    |> clean_up_snapshots()
  end

  def latest_snapshot_index(%__MODULE__{} = state) do
    state.snapshots
    |> Map.keys()
    |> Enum.max()
  end

  # this is a separate function to prepare for async snapshot support
  defp clean_up_snapshots(%__MODULE__{} = state) do
    if state.machine.__craft_mutable__() do
      # a follower should never be downloading one of these, since we only snapshot when all followers are caught up
      [current_snapshot_index | deleteable_indexes] =
        state.snapshots
        |> Map.keys()
        |> Enum.sort(:desc)

      for index <- deleteable_indexes do
        {path, _} = Map.fetch!(state.snapshots, index)

        Application.get_env(:craft, :data_dir)
        |> Path.join(path)
        |> File.rm_rf()
      end

      %{state | snapshots: Map.take(state.snapshots, [current_snapshot_index])}
    else
      state
    end
  end

  def logger_metadata(%__MODULE__{} = state, extras \\ []) do
    color =
      case state.state do
        :lonely ->
          :light_red

        :receiving_snapshot ->
          :magenta

        :follower ->
          :cyan

        :candidate ->
          :blue

        :leader ->
          :green
      end

    # elixir uses the :time keyword, we want a higher resolution timestamp
    Keyword.merge([name: state.name, term: state.current_term, ansi_color: color, t: Time.utc_now(), node: node()], extras)
  end
end
