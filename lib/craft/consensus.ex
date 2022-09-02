defmodule Craft.Consensus do
  @moduledoc false

  alias Craft.RPC
  alias Craft.RPC.RequestVote
  alias Craft.RPC.AppendEntries

  require Logger

  @behaviour :gen_statem

  @follower_timeout 1500
  @follower_jitter 1500
  @election_timeout 1500
  @election_jitter 1500
  @heartbeat_interval 1400

  defmodule State do
    defstruct [
      :name,
      {:other_nodes, []},
      {:state, :follower},
      {:current_term, -1},

      # follower state
      # TODO: move to durable storage
      :voted_for,
      :leader_id,

      # candidate state
      :votes

      # leader state
    ]
  end


  def callback_mode, do: [:state_functions, :state_enter]
  def name(name), do: Module.concat(__MODULE__, name)

  def start_link(name, other_nodes) do
    :gen_statem.start_link({:local, name(name)}, __MODULE__, [name, other_nodes], [])
  end

  def init([name, other_nodes]) do
    Logger.metadata(name: name, node: node())

    {:ok, :follower, %State{name: name, other_nodes: other_nodes}}
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, args}
    }
  end


  def follower(:enter, _previous_state, data) do
    data = %State{data | state: :follower, voted_for: nil, leader_id: nil}
    IO.inspect data

    Logger.info("became follower", logger_metadata(data))

    {:keep_state, data, [follower_state_timeout()]}
  end
  def follower(:state_timeout, :become_candidate, data), do: {:next_state, :candidate, data}

  # ignore any messages from earlier terms
  def follower(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.info("ignoring message #{inspect msg} from #{msg.candidate_id} for earlier term #{term}", logger_metadata(data))

    :keep_state_and_data
  end

  # vote for candidate if their term is higher than ours
  def follower(:cast, %RequestVote{term: term} = request_vote, %State{current_term: current_term} = data) when term > current_term do
    data = %State{data | current_term: term, voted_for: request_vote.candidate_id}

    Logger.info("voting for #{request_vote.candidate_id} and restarting timer", logger_metadata(data))

    RPC.respond_vote(request_vote, data)

    {:keep_state, data, [follower_state_timeout()]}
  end

  # vote for candidate in our term if we haven't voted for anyone else
  def follower(:cast, %RequestVote{term: term} = request_vote, %State{voted_for: nil, current_term: term} = data) do
    data = %State{data | current_term: term, voted_for: request_vote.candidate_id}

    Logger.info("voting for #{request_vote.candidate_id} and restarting timer", logger_metadata(data))

    RPC.respond_vote(request_vote, data)

    {:keep_state, data, [follower_state_timeout()]}
  end

  # repeat vote if response is lost
  def follower(:cast, %RequestVote{candidate_id: candidate_id} = request_vote, %State{voted_for: candidate_id} = data) do
    Logger.info("repeating vote for #{candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, data)

    :keep_state_and_data
  end

  # refuse to vote for anyone if we've already voted
  def follower(:cast, %RequestVote{} = request_vote, data) do
    Logger.info("denying vote to #{request_vote.candidate_id}, already voted for #{data.voted_for}", logger_metadata(data))

    RPC.respond_vote(request_vote, data)

    :keep_state_and_data
  end

  def follower(:cast, %AppendEntries{} = append_entries, %State{} = data) do
    data = %State{data | leader_id: append_entries.leader_id, current_term: append_entries.term}

    Logger.info("leader heartbeat from #{append_entries.leader_id}, restarting timer", logger_metadata(data))

    {:keep_state, data, [follower_state_timeout()]}
  end


  # become candidate and request votes from all other members
  def candidate(:enter, _previous_state, data) do
    election_timeout = @election_timeout + :rand.uniform(@election_jitter)
    data =
      %State{
        data |
        current_term: data.current_term + 1,
        state: :candidate,
        votes: MapSet.new([node()]),
        voted_for: node(),
        leader_id: nil
      }

    Logger.info("became candidate", logger_metadata(data, timeout: election_timeout))

    RPC.request_vote(data)

    {:keep_state, data, [{:state_timeout, election_timeout, :election_failed}]}
  end
  def candidate(:state_timeout, :election_failed, data), do: {:next_state, :candidate, data}

  # ignore messages from earlier terms
  def candidate(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.info("ignoring message #{inspect msg} from #{msg.candidate_id} for earlier term #{term}", logger_metadata(data))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def candidate(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term, do: become_follower(msg, data)

  # if a competing candidate becomes leader, convert to follower and process the AppendEntries
  # def candidate(:cast, %AppendEntries{term: term}, {%State{current_term: current_term}, _} = data) do

  # handle incoming RequestVote response, becoming leader if a quorum votes for us
  def candidate(:cast, %RequestVote.Results{voted_for: voted_for} = results, data) when voted_for == node() do
    Logger.info("vote received from #{results.candidate_id}", logger_metadata(data))

    votes = MapSet.put(data.votes, results.candidate_id)
    data = %State{data | votes: votes}

    num_members = length(data.other_nodes) + 1
    quorum_needed = div(num_members, 2) + 1

    if MapSet.size(votes) >= quorum_needed do
      {:next_state, :leader, data}
    else
      {:keep_state, data}
    end
  end

  def candidate(:cast, %RequestVote.Results{} = results, data) do
    Logger.info("vote denied by #{results.candidate_id}", logger_metadata(data))

    :keep_state_and_data
  end

  def leader(:enter, _previous_state, data) do
    data = %State{data | state: :leader, votes: nil}

    Logger.info("became leader", logger_metadata(data))

    {:keep_state, data, [{{:timeout, :heartbeat}, 0, :heartbeat}]}
  end

  def leader({:timeout, :heartbeat}, :heartbeat, data) do
    Logger.info("heartbeat", logger_metadata(data))

    RPC.append_entries(data)

    {:keep_state_and_data, [{{:timeout, :heartbeat}, @heartbeat_interval, :heartbeat}]}
  end

  # ignore any messages from earlier terms
  def leader(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.info("ignoring message #{inspect msg} from #{msg.candidate_id} for earlier term #{term}", logger_metadata(data))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def leader(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term, do: become_follower(msg, data)


  def leader(:cast, :step_down, data) do
    Logger.info("stepping down", logger_metadata(data))

    {:next_state, :follower, data, [{{:timeout, :heartbeat}, :infinity, :heartbeat}]}
  end

  def leader(:cast, msg, data) do
    Logger.info("ignoring message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end

  defp logger_metadata(%State{state: state, current_term: term}, extras \\ []) do
    color =
      case state do
        :follower ->
          :cyan

        :candidate ->
          :blue

        :leader ->
          :green
      end

    Keyword.merge([state: state, term: term, ansi_color: color], extras)
  end

  defp follower_state_timeout do
    follower_timeout = @follower_timeout + :rand.uniform(@follower_jitter)

    {:state_timeout, follower_timeout, :become_candidate}
  end

  defp become_follower(%{term: term} = msg, data) do
    Logger.info("received message #{inspect msg} from #{msg.candidate_id} from later term #{term}, becoming follower", logger_metadata(data))

    {:next_state, :follower, %State{data | current_term: term}}
  end
end
