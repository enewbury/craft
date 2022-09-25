defmodule Craft.Consensus do
  @moduledoc false

  alias Craft.Log
  alias Craft.RPC
  alias Craft.RPC.RequestVote
  alias Craft.RPC.AppendEntries
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.LeaderState

  require Logger

  @behaviour :gen_statem

  @follower_timeout 1500
  @follower_jitter 1500
  @election_timeout 1500
  @election_jitter 1500
  @heartbeat_interval 1400


  def callback_mode, do: [:state_functions, :state_enter]
  def name(name), do: Module.concat(__MODULE__, name)

  def start_link(name, other_nodes, log_module) do
    :gen_statem.start_link({:local, name(name)}, __MODULE__, [name, other_nodes, log_module], [])
  end

  if Mix.env() == :test do
    def start_link(state), do: :gen_statem.start_link({:local, name(state.name)}, __MODULE__, state, [])
    def ready_to_test(:enter, _, _data), do: :keep_state_and_data
    def ready_to_test(:cast, :run, %FollowerState{} = data), do: {:next_state, :follower, data, []}
    def ready_to_test(:cast, :run, %CandidateState{} = data), do: {:next_state, :candidate, data, []}
    def ready_to_test(:cast, :run, %LeaderState{} = data), do: {:next_state, :leader, data, []}
    def init(data) when is_map(data) do
      me = self()
      spawn_link(fn ->
        :erlang.trace(me, true, [:call])
        :erlang.trace_pattern({__MODULE__, :leader, :_}, true, [:global])
        :erlang.trace_pattern({__MODULE__, :candidate, :_}, true, [:global])
        :erlang.trace_pattern({__MODULE__, :follower, :_}, true, [:global])

        (fn recursor ->
          recursor.(recursor)
        end).(fn loop ->
          receive do
            msg ->
              send(data.tracer_pid, msg)
              loop.(loop)
          end
        end)
      end)

      {:ok, :ready_to_test, data}
    end
  end

  def init([name, other_nodes, log_module]) do
    Logger.metadata(name: name, node: node())

    data =
      %FollowerState{
        name: name,
        other_nodes: other_nodes,
        log: Log.new(name, log_module)
      }

    {:ok, :follower, data}
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, args}
    }
  end


  def follower(:enter, _previous_state, data) do
    data = FollowerState.new(data)

    Logger.info("became follower", logger_metadata(data))

    {:keep_state, data, [follower_state_timeout()]}
  end
  def follower(:state_timeout, :become_candidate, data), do: {:next_state, :candidate, data}

  # if we receive a message with a higher term, bump our term and process the message
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def follower(:cast, %{term: term} = msg, %FollowerState{current_term: current_term} = data) when term > current_term do
    # wipe voted_for etc...
    data = FollowerState.new(%FollowerState{data | current_term: term})

    follower(:cast, msg, data)
  end

  # maybe vote for candidate
  def follower(:cast, %RequestVote{} = request_vote, data) do
    {vote_granted, data} = FollowerState.vote(data, request_vote)

    Logger.info("considering voting for #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  # hear leader heartbeat and append entries if necessary
  def follower(:cast, %AppendEntries{} = append_entries, data) do
    {success, data} = FollowerState.append_entries(data, append_entries)

    RPC.respond_append_entries(append_entries, success, data)

    Logger.info("leader heartbeat from #{append_entries.leader_id}, restarting timer", logger_metadata(data))

    {:keep_state, data, [follower_state_timeout()]}
  end

  def follower(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end

  # become candidate and request votes from all other members
  def candidate(:enter, _previous_state, data) do
    election_timeout = @election_timeout + :rand.uniform(@election_jitter)
    data = CandidateState.new(data)

    Logger.info("became candidate", logger_metadata(data))

    RPC.request_vote(data)

    {:keep_state, data, [{:state_timeout, election_timeout, :election_failed}]}
  end
  def candidate(:state_timeout, :election_failed, data) do
    Logger.info("repeating state", logger_metadata(data))

    :repeat_state_and_data
  end

  # ignore messages from earlier terms
  def candidate(:cast, %{term: term} = msg, %CandidateState{current_term: current_term} = data) when term < current_term do
    Logger.info("ignoring message #{inspect msg} from #{msg.candidate_id} for earlier term #{term}", logger_metadata(data))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def candidate(:cast, %{term: term} = msg, %CandidateState{current_term: current_term} = data) when term > current_term, do: become_follower(msg, data)

  # refuse to vote for another candidate
  def candidate(:cast, %RequestVote{} = request_vote, data) do
    Logger.info("denying vote to other candidate #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, false, data)

    :keep_state_and_data
  end

  # if a competing candidate becomes leader, convert to follower and process the AppendEntries
  def candidate(:cast, %AppendEntries{}, data) do
    {:next_state, :follower, data, [:postpone]}
  end

  # handle incoming RequestVote response, becoming leader if a quorum votes for us
  def candidate(:cast, %RequestVote.Results{} = results, data) do
    if results.vote_granted do
      Logger.info("vote granted by #{results.from}", logger_metadata(data))
    else
      Logger.info("vote denied by #{results.from}", logger_metadata(data))
    end

    data = CandidateState.record_vote(data, results)

    if CandidateState.won_election?(data) do
      {:next_state, :leader, data}
    else
      {:keep_state, data}
    end
  end

  def candidate(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end


  def leader(:enter, _previous_state, data) do
    data = LeaderState.new(data)

    Logger.info("became leader", logger_metadata(data))

    {:keep_state, data, [{:state_timeout, 0, :heartbeat}]}
  end

  def leader(:state_timeout, :heartbeat, data) do
    Logger.info("heartbeat", logger_metadata(data))

    RPC.append_entries(data)

    {:keep_state_and_data, [{:state_timeout, @heartbeat_interval, :heartbeat}]}
  end

  # ignore any messages from earlier terms
  def leader(:cast, %{term: term} = msg, %LeaderState{current_term: current_term} = data) when term < current_term do
    Logger.info("ignoring message #{inspect msg} for earlier term #{term}", logger_metadata(data))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def leader(:cast, %{term: term} = msg, %LeaderState{current_term: current_term} = data) when term > current_term, do: become_follower(msg, data)

  # ignore superfluous votes from when we were a candidate
  def leader(:cast, %RequestVote.Results{}, _data), do: :keep_state_and_data

  def leader(:cast, %AppendEntries.Results{} = results, data) do
    {:keep_state, LeaderState.handle_append_entries_results(data, results)}
  end

  def leader(:cast, :step_down, data) do
    Logger.info("stepping down", logger_metadata(data))

    {:next_state, :follower, data, []}
  end

  def leader(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end


  defp logger_metadata(%state{current_term: term}, extras \\ []) do
    color =
      case state do
        FollowerState ->
          :cyan

        CandidateState ->
          :blue

        LeaderState ->
          :green
      end

    Keyword.merge([term: term, ansi_color: color], extras)
  end

  defp follower_state_timeout do
    follower_timeout = @follower_timeout + :rand.uniform(@follower_jitter)

    {:state_timeout, follower_timeout, :become_candidate}
  end

  defp become_follower(%{term: term} = msg, data) do
    Logger.info("received message #{inspect msg} from #{msg.candidate_id} from later term #{term}, becoming/remaining follower", logger_metadata(data))

    {:next_state, :follower, %{data | current_term: term}, [:postpone]}
  end
end
