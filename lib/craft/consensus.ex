defmodule Craft.Consensus do
  @moduledoc false

  #
  # follower -> lonely follower -> pre vote -> candidate -> leader
  #
  # follower: happily following leader
  # lonely follower: hasn't heart from leader in a while, would be willing to vote 'yes' in a pre-vote
  # pre-vote follower: follower that's initiated a pre-vote
  # candidate: follower that's called an election after a successful majority pre-vote
  # leader: candidate that's won majority vote
  #

  alias Craft.Consensus.State
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.LonelyFollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.LeaderState
  alias Craft.Log
  alias Craft.Log.Entry
  alias Craft.Machine
  alias Craft.RPC
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.RequestVote

  require Logger

  @behaviour :gen_statem

  @heartbeat_interval 1000
  @follower_lonely_timeout @heartbeat_interval + 300
  @election_timeout 1500 # amount of time to wait for votes before concluding that the election has failed

  defp jitter(max \\ 1500), do: :rand.uniform(max)

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
      def ready_to_test(:cast, :run, %State{mode_state: %FollowerState{}} = data), do: {:next_state, :follower, data, []}
      def ready_to_test(:cast, :run, %State{mode_state: %CandidateState{}} = data), do: {:next_state, :candidate, data, []}
      def ready_to_test(:cast, :run, %State{mode_state: %LeaderState{}} = data), do: {:next_state, :leader, data, []}
      def ready_to_test({:call, _from}, :catch_up, _data), do: {:keep_state_and_data, [:postpone]}
      for state <- [:follower, :lonely_follower, :prevote, :candidate, :leader] do
        def unquote(state)(event, msg, data) do
          send(data.nexus_pid, {:trace, DateTime.utc_now(), node(), unquote(state), event, msg, data})
          apply(Craft.Consensus, unquote(state), [event, msg, data])
        end
      end
    end
  end

  def init(args) do
    Logger.metadata(name: args.name, node: node())

    data =
      %State{
        name: args.name,
        other_nodes: args.other_nodes,
        log: Log.new(args.name, args.log_module)
      }

    Logger.info("started")

    {:ok, :lonely_follower, data}
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, args}
    }
  end

  #
  # Lonely Follower
  #

  def lonely_follower(:enter, _previous_state, data) do
    data = LonelyFollowerState.new(data)

    Logger.info("became lonely", logger_metadata(data))

    {:keep_state, data, [{:state_timeout, jitter(), :begin_pre_vote}]}
  end

  def lonely_follower(:state_timeout, :begin_pre_vote, data) do
    Logger.info("pre-vote started", logger_metadata(data))

    RPC.request_vote(data, pre_vote: true)

    {:keep_state_and_data, [{:state_timeout, @election_timeout, :election_failed}]}
  end

  def lonely_follower(:state_timeout, :election_failed, data) do
    Logger.info("pre-vote failed, repeating state", logger_metadata(data))

    :repeat_state_and_data
  end

  # if we receive a message with a higher term, bump our term and return to follower state
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def lonely_follower(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term do
    data = LonelyFollowerState.new(%State{data | current_term: term})

    lonely_follower(:cast, msg, data)
  end


  def lonely_follower(:cast, %RequestVote{pre_vote: true} = request_vote, data) do
    vote_granted = FollowerState.vote_for?(data, request_vote)

    Logger.info("considering pre-voting for #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  def lonely_follower(:cast, %RequestVote{pre_vote: false} = request_vote, data) do
    {vote_granted, data} = LonelyFollowerState.vote(data, request_vote)

    Logger.info("considering voting for #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  def lonely_follower(:cast, %RequestVote.Results{pre_vote: true} = results, data) do
    if results.vote_granted do
      Logger.info("pre-vote granted by #{results.from}", logger_metadata(data))
    else
      Logger.info("pre-vote denied by #{results.from}", logger_metadata(data))
    end

    data = LonelyFollowerState.record_vote(data, results)

    case LonelyFollowerState.election_result(data) do
      :won ->
        {:next_state, :candidate, data}

      :lost ->
        :repeat_state_and_data

      :pending ->
        {:keep_state, data}
    end
  end

  def lonely_follower(:cast, %AppendEntries{}, data) do
    {:next_state, :follower, data, [:postpone]}
  end

  def lonely_follower({:call, from}, :catch_up, data) do
    {:keep_state_and_data, [{:reply, from, {data.commit_index, data.log}}]};
  end

  #
  # Follower
  #

  def follower(:enter, _previous_state, data) do
    data = FollowerState.new(data)

    Logger.info("became follower", logger_metadata(data))

    {:keep_state, data, [become_lonely_follower_timeout()]}
  end
  def follower(:state_timeout, :become_lonely, data), do: {:next_state, :lonely_follower, data}

  # if we receive a message with a higher term, bump our term and process the message
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def follower(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term do
    # wipe voted_for etc...
    data = FollowerState.new(%State{data | current_term: term})

    follower(:cast, msg, data)
  end

  # followers are happy with the leader, they vote "no" in pre-votes
  def follower(:cast, %RequestVote{pre_vote: true} = request_vote, data) do
    Logger.info("denying pre-vote to #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, false, data)

    :keep_state_and_data
  end

  def follower(:cast, %RequestVote{pre_vote: false} = request_vote, data) do
    {vote_granted, data} = FollowerState.vote(data, request_vote)

    Logger.info("considering pre-voting for #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
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

    {:keep_state, data, [become_lonely_follower_timeout()]}
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

  #
  # Candidate
  #

  # become candidate and request votes from all other members
  def candidate(:enter, _previous_state, data) do
    election_timeout = @election_timeout + jitter()
    data = CandidateState.new(data)

    Logger.info("became candidate", logger_metadata(data))

    RPC.request_vote(data, pre_vote: false)

    {:keep_state, data, [{:state_timeout, election_timeout, :election_failed}]}
  end
  def candidate(:state_timeout, :election_failed, data) do
    Logger.info("election failed, repeating state", logger_metadata(data))

    :repeat_state_and_data
  end

  # ignore messages from earlier terms
  def candidate(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.info("ignoring message #{inspect msg} for earlier term #{term}", logger_metadata(data))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def candidate(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term, do: become_follower(msg, data)

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

    case CandidateState.election_result(data) do
      :won ->
        {:next_state, :leader, data}

      :lost ->
        :repeat_state_and_data

      :pending ->
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

  # this should only happen in test, it'd be nice to throw a Mix.env() assertion in here,
  # but Mix isn't available in releases.
  def candidate({:call, from}, :catch_up, data) do
    {:keep_state_and_data, [{:reply, from, {data.commit_index, data.log}}]};
  end

  def candidate(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end

  #
  # Leader
  #

  def leader(:enter, _previous_state, data) do
    data = LeaderState.new(data)

    entry = %Entry{term: data.current_term}
    log = Log.append(data.log, entry)
    data = %State{data | log: log}

    Logger.info("became leader", logger_metadata(data))

    {:keep_state, data, [{:state_timeout, 0, :heartbeat}]}
  end

  def leader(:state_timeout, :heartbeat, data) do
    Logger.debug("heartbeat", logger_metadata(data))

    RPC.append_entries(data)

    {:keep_state_and_data, [{:state_timeout, @heartbeat_interval, :heartbeat}]}
  end

  # ignore any messages from earlier terms
  def leader(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.info("ignoring message #{inspect msg} for earlier term #{term}", logger_metadata(data))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def leader(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term, do: become_follower(msg, data)

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
    client_requests = Map.put(data.mode_state.client_requests, entry_index, id)

    {:keep_state, %State{data | log: log, mode_state: %LeaderState{data.mode_state | client_requests: client_requests}}}
  end

  def leader(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end


  defp logger_metadata(%State{} = state, extras \\ []) do
    color =
      case state.mode_state do
        %FollowerState{} ->
          :cyan

        {_, _} ->
          :yellow

        %CandidateState{} ->
          :blue

        %LeaderState{} ->
          :green
      end

    Keyword.merge([term: state.current_term, ansi_color: color], extras)
  end

  defp become_lonely_follower_timeout do
    {:state_timeout, @follower_lonely_timeout, :become_lonely}
  end

  defp become_follower(%{term: term} = msg, data) do
    Logger.info("received message #{inspect msg} from later term #{term}, becoming/remaining follower", logger_metadata(data))

    {:next_state, :follower, %{data | current_term: term}, [:postpone]}
  end
end
