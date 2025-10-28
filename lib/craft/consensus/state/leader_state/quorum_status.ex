defmodule Craft.Consensus.State.LeaderState.QuorumStatus do
  alias Craft.Consensus.State
  alias Craft.RPC.AppendEntries

  @num_rounds 10

  defstruct [:current_round_sent_at, current_round_successful: false, rounds: %{}]

  def new do
    %__MODULE__{}
  end

  def start_new_round(%State{} = state) do
    quorum_status = state.leader_state.quorum_status

    # the monotonic clock is not strictly increasing, so it can freeze unboundedly.
    # if that happens, we add a millisecond to the last round's value to continue generating unique consensus round ids
    now = :erlang.monotonic_time(:millisecond)
    round_sent_at =
      if !quorum_status.current_round_sent_at || quorum_status.current_round_sent_at < now do
        now
      else
        quorum_status.current_round_sent_at + 1
      end

    # drop old rounds
    rounds =
      quorum_status.rounds
      |> Enum.sort_by(fn {round_sent_at, _} -> round_sent_at end, :desc)
      |> Enum.take(@num_rounds)
      |> Map.new()

    quorum_status =
      %{
        quorum_status |
          current_round_successful: false,
          current_round_sent_at: round_sent_at,
          rounds: Map.put(rounds, round_sent_at, %{})
      }

    put_in(state.leader_state.quorum_status, quorum_status)
  end

  # returns {this_vote_caused_quorum :: boole, state}
  def heartbeat_response_received(%State{} = state, %AppendEntries.Results{} = results) do
    cond do
      !state.leader_state.quorum_status.rounds[results.heartbeat_sent_at] ->
        # Logger.warning("unknown/old quorum round in heartbeat reply: #{inspect results}, ignoring.", logger_metadata(state))

        {false, state}

      state.leader_state.quorum_status.rounds[results.heartbeat_sent_at][results.from] ->
        # Logger.warning("duplicate heartbeat reply received: #{inspect results}, ignoring.", logger_metadata(state))

        {false, state}

      true ->
        state = put_in(state.leader_state.quorum_status.rounds[results.heartbeat_sent_at][results.from], {results.heartbeat_sent_at, :erlang.monotonic_time(:millisecond)})

        if results.heartbeat_sent_at == state.leader_state.quorum_status.current_round_sent_at
           and not state.leader_state.quorum_status.current_round_successful
           and round_successful?(state, results.heartbeat_sent_at) do
          state = put_in(state.leader_state.quorum_status.current_round_successful, true)

          {true, state}
        else
          {false, state}
        end
    end
  end

  def last_quorum_at(%State{} = state) do
    quorum_status = state.leader_state.quorum_status

    quorum_status.rounds
    |> Map.keys()
    |> Enum.sort(:desc)
    |> Enum.find(&round_successful?(state, &1))
  end

  defp round_successful?(%State{} = state, round_sent_at) do
    # -1 since we're the leader
    num_replies_needed = State.quorum_needed(state) - 1

    num_replies_received =
      state.leader_state.quorum_status.rounds
      |> Map.get(round_sent_at)
      |> Enum.count()

    num_replies_received >= num_replies_needed
  end
end
