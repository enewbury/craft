defmodule Craft.Consensus do
  @moduledoc false

  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.LeaderState
  alias Craft.Log
  alias Craft.Log.Entry
  alias Craft.Machine
  alias Craft.RPC
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.RequestVote

  require Logger

  @behaviour :gen_statem

  @follower_timeout 1500
  @follower_jitter 1500
  @election_timeout 1500
  @election_jitter 1500
  @heartbeat_interval 1400


  #
  # API
  #

  def command(name, node, command, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    id = {self(), make_ref()}

    # FIXME: use call instead
    :gen_statem.cast({name(name), node}, {:command, id, command})

    receive do
      {^id, reply} ->
        reply

      after
        timeout ->
          {:error, :timeout}
    end
  end

  def name(name), do: Module.concat(__MODULE__, name)

  # called after the machine restarts to get any committed entries that need to be applied
  def catch_up(name) do
    :gen_statem.call({name(name), node()}, :catch_up)
  end

  # TODO: pre-compute quorum and store in state
  def quorum_reached?(state, num) do
    num_members = length(state.other_nodes) + 1
    quorum_needed = div(num_members, 2) + 1

    num >= quorum_needed
  end

  #
  # genstatem implementation
  #

  def callback_mode, do: [:state_functions, :state_enter]

  def start_link(args) do
    :gen_statem.start_link({:local, name(args.name)}, __MODULE__, args, [])
  end

  if Mix.env() == :test do
    defoverridable start_link: 1
    def start_link(state), do: :gen_statem.start_link({:local, name(state.name)}, Craft.Consensus.Tracer, state, [])
    defmodule Tracer do
      @moduledoc ":erlang.trace/3 can't guarantee delivery order between traces messages and messages that this process sends, so we decorate instead"
      defdelegate callback_mode, to: Craft.Consensus
      def init(data) when is_struct(data) do
        {:ok, :ready_to_test, data}
      end
      def ready_to_test(:enter, _, _data), do: :keep_state_and_data
      def ready_to_test(:cast, :run, %FollowerState{} = data), do: {:next_state, :follower, data, []}
      def ready_to_test(:cast, :run, %CandidateState{} = data), do: {:next_state, :candidate, data, []}
      def ready_to_test(:cast, :run, %LeaderState{} = data), do: {:next_state, :leader, data, []}
      for state <- [:follower, :candidate, :leader] do
        def unquote(state)(event, msg, data) do
          send(data.tracer_pid, {:trace, DateTime.utc_now(), node(), unquote(state), event, msg, data})
          apply(Craft.Consensus, unquote(state), [event, msg, data])
        end
      end
    end
  end

  def init(args) do
    Logger.metadata(name: args.name, node: node())

    data =
      %FollowerState{
        name: args.name,
        other_nodes: args.other_nodes,
        log: Log.new(args.name, args.log_module)
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

    #
    # reset timer if we grant vote to give candidate a sec to take leadership
    #
    # if our timer pops immediately after granting a vote, we'll become a candidate
    # and cause an unnecessary election
    #
    if vote_granted do
      {:keep_state, data, [follower_state_timeout()]}
    else
      {:keep_state, data}
    end
  end

  # hear leader heartbeat and append entries if necessary
  def follower(:cast, %AppendEntries{} = append_entries, data) do
    old_commit_index = data.commit_index
    {success, data} = FollowerState.append_entries(data, append_entries)

    if success && data.commit_index > old_commit_index do
      Machine.commit_index_bumped(data)
    end

    RPC.respond_append_entries(append_entries, success, data)

    Logger.debug("leader heartbeat from #{append_entries.leader_id}, restarting timer", logger_metadata(data))

    {:keep_state, data, [follower_state_timeout()]}
  end

  def follower(:cast, {:command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, {:error, {:not_leader, data.leader_id}}})

    :keep_state_and_data
  end

  def follower({:call, from}, :catch_up, data) do
    {:keep_state_and_data, [{:reply, from, {data.commit_index, data.log}}]};
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

  # even though this node doesn't recognize the leader as legitimate anymore,
  # we should still try to redirect commands to the old leader, in case it is
  # actually is legitimate and we're incorrect. (e.g. we're isolated from the
  # other nodes and they're happily carrying on without us)
  def candidate(:cast, {:command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, {:error, {:not_leader, data.leader_id}}})

    :keep_state_and_data
  end

  def candidate(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end


  def leader(:enter, _previous_state, data) do
    data = LeaderState.new(data)

    entry = %Entry{term: data.current_term}
    log = Log.append(data.log, entry)
    data = %LeaderState{data | log: log}

    Logger.info("became leader", logger_metadata(data))

    {:keep_state, data, [{:state_timeout, 0, :heartbeat}]}
  end

  def leader(:state_timeout, :heartbeat, data) do
    Logger.debug("heartbeat", logger_metadata(data))

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
    old_commit_index = data.commit_index
    data = LeaderState.handle_append_entries_results(data, results)

    if data.commit_index > old_commit_index do
      Machine.commit_index_bumped(data)
    end

    {:keep_state, data}
  end

  def leader(:cast, :step_down, data) do
    Logger.info("stepping down", logger_metadata(data))

    {:next_state, :follower, data, []}
  end

  def leader(:cast, {:command, id, command}, data) do
    entry = %Entry{term: data.current_term, command: command}
    log = Log.append(data.log, entry)

    entry_index = Log.latest_index(log)
    client_requests = Map.put(data.client_requests, entry_index, id)

    {:keep_state, %LeaderState{data | log: log, client_requests: client_requests}}
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
    Logger.info("received message #{inspect msg} from #{msg.from} from later term #{term}, becoming/remaining follower", logger_metadata(data))

    {:next_state, :follower, %{data | current_term: term}, [:postpone]}
  end
end
