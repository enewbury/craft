defmodule Craft.RPC do
  @moduledoc false

  alias Craft.Consensus
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.LeaderState
  alias Craft.RPC.RequestVote
  alias Craft.RPC.AppendEntries

  # def start(name, to_node, {_m, _f, _a} = request) do
  #   ARQ.start(request, supervisor)
  # end

  def request_vote(%CandidateState{} = state) do
    request_vote = RequestVote.new(state)

    for to_node <- state.other_nodes do
      send_message(request_vote, to_node, state)
    end
  end

  def respond_vote(%RequestVote{} = request_vote, vote_granted, state) do
    state
    |> RequestVote.Results.new(vote_granted)
    |> send_message(request_vote.candidate_id, state)
  end

  def append_entries(%LeaderState{} = state) do
    for to_node <- state.other_nodes do
      state
      |> AppendEntries.new(to_node)
      |> send_message(to_node, state)
    end
  end

  def respond_append_entries(%AppendEntries{} = append_entries, success, %FollowerState{} = state) do
    state
    |> AppendEntries.Results.new(success)
    |> send_message(append_entries.leader_id, state)
  end

  def send_message(message, to_node, state) do
    :gen_statem.cast({Consensus.name(state.name), to_node}, message)
  end
end
