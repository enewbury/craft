defmodule Craft.Consensus.State do
  alias Craft.Consensus.State.Members
  alias Craft.Consensus.State.LeaderState
  alias Craft.Consensus.State.Election
  alias Craft.Persistence
  alias Craft.RPC.RequestVote

  defstruct [
    :state,
    :name,
    :members,
    :persistence,
    :nexus_pid,
    :leader_id,
    {:current_term, -1},
    {:commit_index, 0},

    :leader_state,
    :voted_for, # lonely and follower
    :election, # lonely and candidate
    :leadership_transfer_request_id # candidate only
  ]

  def new(name, nodes, persistence) do
    persistence = Persistence.new(name, persistence)
    voted_for = Persistence.get_voted_for!(persistence)
    current_term = Persistence.get_current_term!(persistence) || -1

    %__MODULE__{
      current_term: current_term,
      name: name,
      members: Members.new(nodes),
      persistence: persistence,
      voted_for: voted_for
    }
  end

  def become_lonely(%__MODULE__{} = state) do
    %__MODULE__{
      state |
      state: :lonely,
      leader_state: nil,
      leadership_transfer_request_id: nil,
      election: Election.new(state.members)
    }
  end

  def become_follower(%__MODULE__{} = state) do
    %__MODULE__{
      state |
      state: :follower,
      leader_state: nil,
      leadership_transfer_request_id: nil,
      election: nil
    }
  end

  def become_candidate(%__MODULE__{} = state, leadership_transfer_request_id \\ nil) do
    %__MODULE__{
      state |
      state: :candidate,
      leader_state: nil,
      leadership_transfer_request_id: leadership_transfer_request_id,
      election: Election.new(state.members)
    }
    |> set_current_term(state.current_term + 1)
    |> set_voted_for(node())
  end

  def become_leader(%__MODULE__{} = state) do
    %__MODULE__{
      state |
      state: :leader,
      leader_id: node(),
      leader_state: LeaderState.new(state),
      election: nil
    }
  end

  # voting for others

  def vote_for?(%__MODULE__{current_term: current_term}, %RequestVote{term: term}) when term < current_term, do: false

  def vote_for?(%__MODULE__{} = state, %RequestVote{} = request_vote) do
    request_vote.last_log_term > Persistence.latest_term(state.persistence) ||
    (request_vote.last_log_term == Persistence.latest_term(state.persistence) &&
      request_vote.last_log_index >= Persistence.latest_index(state.persistence))
  end

  def vote(%__MODULE__{voted_for: nil} = state, %RequestVote{} = request_vote) do
    if vote_for?(state, request_vote) do
      {true, set_voted_for(state, request_vote.candidate_id)}
    else
      {false, state}
    end
  end

  # repeat vote if asked
  def vote(%__MODULE__{} = state, %RequestVote{} = request_vote) do
    {state.voted_for == request_vote.candidate_id, state}
  end

  # holding pre-vote and leadership elections

  def record_vote(%__MODULE__{} = state, %RequestVote.Results{} = results) do
    %__MODULE__{state | election: Election.record_vote(state.election, results)}
  end

  def election_result(%__MODULE__{} = state) do
    Election.election_result(state.election, quorum_needed(state))
  end



  # TODO: pre-compute quorum and cache
  def quorum_needed(%__MODULE__{} = state) do
    num_members = MapSet.size(state.members.voting_nodes) + 1

    div(num_members, 2) + 1
  end

  def logger_metadata(%__MODULE__{} = state, extras \\ []) do
    # color =
    #   node()
    #   |> :erlang.phash2(255)
    #   |> IO.ANSI.color()

    color =
      case state.state do
        :lonely ->
          :light_red

        :follower ->
          :cyan

        :candidate ->
          :blue

        :leader ->
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

    %__MODULE__{state | persistence: persistence, voted_for: voted_for}
  end
end
