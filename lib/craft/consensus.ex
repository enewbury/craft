defmodule Craft.Consensus do
  @moduledoc false

  #
  # follower -> lonely -> candidate -> leader
  #
  # follower: happily following leader
  # lonely: hasn't heart from leader in a while, would be willing to vote 'yes' in a pre-vote
  # candidate: follower that's called an election after a successful majority pre-vote
  # leader: candidate that's won majority vote
  #

  alias Craft.Consensus.State
  alias Craft.Consensus.State.Members
  alias Craft.Consensus.LonelyState
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.LeaderState
  alias Craft.Consensus.LeaderState.LeadershipTransfer
  alias Craft.Consensus.LeaderState.MembershipChange
  alias Craft.Log
  alias Craft.Log.CommandEntry
  alias Craft.Log.MembershipEntry
  alias Craft.Machine
  alias Craft.RPC
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.RequestVote

  require Logger

  import State, only: [logger_metadata: 1]

  @behaviour :gen_statem

  @heartbeat_interval 1000
  @lonely_timeout @heartbeat_interval + 300
  # amount of time to wait for votes before concluding that the election has failed
  @election_timeout 1500
  # amount of time to wait for new leader to take over before concluding that leadership transfer has failed
  @leadership_transfer_timeout 3000

  defp jitter(max \\ 1500), do: :rand.uniform(max)

  #
  # API
  #

  def command(name, node, command, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    id = {self(), make_ref()}

    # FIXME: use call instead
    :gen_statem.cast({name(name), node}, {:machine_command, id, command})

    receive do
      {^id, reply} ->
        reply

      after
        timeout ->
          {:error, :timeout}
    end
  end

  def cast_user_command(name, node, msg, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    id = {self(), make_ref()}

    :gen_statem.cast({name(name), node}, {:user_command, id, msg})

    receive do
      {^id, reply} ->
        reply

      after
        timeout ->
          {:error, :timeout}
    end
  end

  def state(name, node) do
    :gen_statem.call({name(name), node}, :state)
  end

  def configuration(name, node) do
    :gen_statem.call({name(name), node}, :configuration)
  end

  def add_member(name, node, member) do
    :gen_statem.call({name(name), node}, {:add_member, member})
  end

  def remove_member(name, node, member) do
    :gen_statem.call({name(name), node}, {:remove_member, member})
  end

  def transfer_leadership(name, node, to_node) do
    cast_user_command(name, node, {:transfer_leadership, to_node})
  end

  # called after the machine restarts to get any committed entries that need to be applied
  def catch_up(name) do
    :gen_statem.call({name(name), node()}, :catch_up)
  end

  def name(name), do: Module.concat(__MODULE__, name)

  def quorum_reached?(state, num) do
    num >= State.quorum_needed(state)
  end

  def vote_for?(%State{current_term: current_term}, %RequestVote{term: term}) when term < current_term, do: false

  def vote_for?(%State{} = state, %RequestVote{} = request_vote) do
    request_vote.last_log_term > Log.latest_term(state.log) ||
    (
      request_vote.last_log_term == Log.latest_term(state.log) &&
        request_vote.last_log_index >= Log.latest_index(state.log)
    )
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
      for state <- [:follower, :lonely, :candidate, :leader] do
        def unquote(state)(event, msg, data) do
          send(data.nexus_pid, {:trace, DateTime.utc_now(), node(), unquote(state), event, msg, data})
          apply(Craft.Consensus, unquote(state), [event, msg, data])
        end
      end
    end
  end

  def init(args) do
    Logger.metadata(name: args.name, node: node())

    data = State.new(args.name, args.nodes, args.log_module)

    Logger.info("started")

    {:ok, :lonely, data}
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, args}
    }
  end

  #
  # Lonely
  #
  # hasn't heard from a leader in a while, ready to:
  #
  # - vote for candidates
  # - hold pre-vote elections as a precursor to becoming a candidate for a true election
  #

  def lonely(:enter, previous_state, data) do
    data = LonelyState.new(data)

    if previous_state != :lonely do
      Logger.info("became lonely", logger_metadata(data))
    end

    {:keep_state, data, [{:state_timeout, jitter(), :begin_pre_vote}]}
  end

  def lonely(:state_timeout, :begin_pre_vote, data) do
    Logger.info("pre-vote started", logger_metadata(data))

    RPC.request_vote(data, pre_vote: true)

    {:keep_state_and_data, [{:state_timeout, @election_timeout, :election_failed}]}
  end

  def lonely(:state_timeout, :election_failed, data) do
    Logger.info("pre-vote failed, repeating state", logger_metadata(data))

    :repeat_state_and_data
  end

  # if we receive a message with a higher term, bump our term and process the message
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def lonely(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term do
    lonely(:cast, msg, LonelyState.new(%State{data | current_term: term}))
  end

  def lonely(:cast, %RequestVote{pre_vote: true} = request_vote, data) do
    vote_granted = vote_for?(data, request_vote)

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} pre-vote to #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  def lonely(:cast, %RequestVote{pre_vote: false} = request_vote, data) do
    {vote_granted, data} = LonelyState.vote(data, request_vote)

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} vote to #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  def lonely(:cast, %RequestVote.Results{pre_vote: true} = results, data) do
    Logger.info("pre-vote #{if results.vote_granted, do: "granted", else: "denied"} by #{results.from}", logger_metadata(data))

    data = LonelyState.record_pre_vote(data, results)

    case LonelyState.pre_vote_election_result(data) do
      :won ->
        {:next_state, :candidate, data}

      :lost ->
        :repeat_state_and_data

      :pending ->
        {:keep_state, data}
    end
  end

  def lonely(:cast, %AppendEntries{}, data) do
    {:next_state, :follower, data, [:postpone]}
  end

  def lonely(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, {:error, {:not_leader, data.leader_id}}})

    :keep_state_and_data
  end

  def lonely(:cast, {:machine_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, {:error, {:not_leader, data.leader_id}}})

    :keep_state_and_data
  end

  def lonely({:call, from}, :catch_up, data) do
    {:keep_state_and_data, [{:reply, from, {data.commit_index, data.log}}]}
  end

  def lonely({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, {:error, {:not_leader, data.leader_id}}}]}
  end

  def lonely(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end

  #
  # Follower
  #
  # hears heartbeats from the leader, appends log entries
  #

  def follower(:enter, _previous_state, data) do
    data = FollowerState.new(data)

    Logger.info("became follower", logger_metadata(data))

    {:keep_state, data, [become_lonely_timeout()]}
  end
  def follower(:state_timeout, :become_lonely, data), do: {:next_state, :lonely, data}

  def follower(:cast, %RequestVote{leadership_transfer: true, term: term} = request_vote, %State{current_term: current_term} = data) when term > current_term do
    data = %State{data | current_term: term}
    {vote_granted, data} = FollowerState.vote(data, request_vote)

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} vote to #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  # followers are happy with the leader, they vote "no" to all non-transfer elections types, regardless of term
  def follower(:cast, %RequestVote{} = request_vote, data) do
    Logger.info("denying #{if request_vote.pre_vote, do: "pre-", else: ""}vote to #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, false, data)

    :keep_state_and_data
  end

  # if we receive an AppendEntries message with a higher term, bump our term and process the message
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def follower(:cast, %AppendEntries{term: term} = msg, %State{current_term: current_term} = data) when term > current_term do
    follower(:cast, msg, %State{data | current_term: term})
  end

  def follower(:cast, %AppendEntries{prev_log_term: prev_log_term} = append_entries, data) do
    old_commit_index = data.commit_index
    data = %State{data | leader_id: append_entries.leader_id}

    {success, data} =
      case Log.fetch(data.log, append_entries.prev_log_index) do
        {:ok, %{term: ^prev_log_term}} ->
          rewound_entries = Log.fetch_from(data.log, append_entries.prev_log_index + 1)

          log =
            data.log
            |> Log.rewind(append_entries.prev_log_index)
            |> Log.append(append_entries.entries)

          data = %State{data | log: log, commit_index: min(append_entries.leader_commit, Log.latest_index(log))}

          new_membership_entry =
            append_entries.entries
            |> Enum.reverse()
            |> Enum.find(fn
              %MembershipEntry{} ->
                true

              _ ->
                false
            end)

          case new_membership_entry do
            %MembershipEntry{members: members} ->
              {true, %State{data | members: members}}

            # if the entries that we've rewound contained a membership entry, and the incoming entries from the
            # leader don't include a new membership entry, we need to look back through the log until we find one
            # to determine the current cluster membership (section 4.1)
            nil ->
              Enum.find_value(rewound_entries, {true, data}, fn
                %MembershipEntry{members: members} ->
                  {true, %State{data | members: members}}

                _ ->
                  false
              end)
          end

        _ ->
          {false, %State{data | log: Log.rewind(data.log, append_entries.prev_log_index)}}
      end

    Logger.debug("leader heartbeat from #{append_entries.leader_id}, restarting timer", logger_metadata(data))

    RPC.respond_append_entries(append_entries, success, data)

    if success && data.commit_index > old_commit_index do
      Machine.commit_index_bumped(data)
    end

    # leader told us to take over leadership when our log is caught up
    if append_entries.leadership_transfer &&
      append_entries.leadership_transfer.latest_index == Log.latest_index(data.log) &&
      append_entries.leadership_transfer.latest_term == Log.latest_term(data.log) do
      {:next_state, :candidate, {data, append_entries.leadership_transfer.from}}
    else
      {:keep_state, data, [become_lonely_timeout()]}
    end
  end

  def follower(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, {:error, {:not_leader, data.leader_id}}})

    :keep_state_and_data
  end

  def follower(:cast, {:machine_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, {:error, {:not_leader, data.leader_id}}})

    :keep_state_and_data
  end

  def follower({:call, from}, :catch_up, data) do
    {:keep_state_and_data, [{:reply, from, {data.commit_index, data.log}}]}
  end

  def follower({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, {:error, {:not_leader, data.leader_id}}}]}
  end

  def follower(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end

  #
  # Candidate
  #

  def candidate(:enter, :follower, {data, leadership_transfer_request_id}) do
    data = CandidateState.new(data, leadership_transfer_request_id)

    Logger.info("became candidate, initiating leadership transfer election", logger_metadata(data))

    RPC.request_vote(data, leadership_transfer: true)

    # TODO: if election fails, send :error to leadership_transfer_request_id
    {:keep_state, data, [{:state_timeout, @election_timeout + jitter(), :election_failed}]}
  end

  def candidate(:enter, _previous_state, data) do
    data = CandidateState.new(data)

    Logger.info("became candidate", logger_metadata(data))

    RPC.request_vote(data)

    {:keep_state, data, [{:state_timeout, @election_timeout + jitter(), :election_failed}]}
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
    Logger.info("denying #{(if request_vote.pre_vote, do: "pre-", else: "")} vote to #{request_vote.candidate_id}", logger_metadata(data))

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
  def candidate(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, {:error, {:not_leader, data.leader_id}}})

    :keep_state_and_data
  end

  def candidate(:cast, {:machine_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, {:error, {:not_leader, data.leader_id}}})

    :keep_state_and_data
  end

  # this should only happen in test, it'd be nice to throw an assertion in here,
  def candidate({:call, from}, :catch_up, data) do
    {:keep_state_and_data, [{:reply, from, {data.commit_index, data.log}}]}
  end

  def candidate({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, {:error, {:not_leader, data.leader_id}}}]}
  end

  def candidate(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end

  #
  # Leader
  #

  def leader(:enter, previous_state, %State{mode_state: %CandidateState{leadership_transfer_request_id: {caller_pid, _ref} = id}} = data) do
    send(caller_pid, {id, :ok})

    data = put_in(data.mode_state.leadership_transfer_request_id, nil)

    leader(:enter, previous_state, data)
  end

  def leader(:enter, _previous_state, data) do
    data = LeaderState.new(data)

    #
    # when the cluster starts up, each node is explicitly given the same configuration to
    # hold in-memory to bootstrap the cluster.
    #
    # however, we need to store the current config in the log so that when nodes restart,
    # they have a way to determine what the current config is (walk backwards from the end of the
    # log looking for the most recent config)
    #
    data = %State{data | log: Log.append(data.log, MembershipEntry.new(data))}

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

  def leader(:cast, %RequestVote{pre_vote: true} = request_vote, data) do
    vote_granted = vote_for?(data, request_vote)

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} pre-vote to #{request_vote.candidate_id}", logger_metadata(data))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  # ignore superfluous votes from when we were a candidate
  def leader(:cast, %RequestVote.Results{}, _data), do: :keep_state_and_data

  def leader(:cast, %AppendEntries.Results{} = results, data) do
    old_commit_index = data.commit_index
    data = LeaderState.handle_append_entries_results(data, results)

    if data.commit_index > old_commit_index do
      Machine.commit_index_bumped(data)
    end

    data =
      Enum.reduce(data.members.catching_up_nodes, data, fn node, data ->
        if Log.latest_index(data.log) - 1 == Map.get(data.mode_state.next_indices, node) do
          Logger.info("node #{inspect node} has caught up", logger_metadata(data))

          data = %State{data | members: Members.allow_node_to_vote(data.members, node)}

          %State{data | log: Log.append(data.log, MembershipEntry.new(data))}
        else
          data
        end
      end)

    # the membership change has committed
    with %MembershipChange{} = membership_change <- data.mode_state.membership_change,
         true <- data.commit_index >= membership_change.log_index do
      data = put_in(data.mode_state.membership_change, nil)
      actions = [{:reply, membership_change.from, :ok}]

      if membership_change.action == :remove && membership_change.node == node() do
        data = LeaderState.transfer_leadership(data)
        actions = [{{:timeout, :leadership_transfer_failed}, @leadership_transfer_timeout, :self_removal} | actions]

        RPC.append_entries(data)

        {:keep_state, data, actions}
      else
        {:keep_state, data, actions}
      end
    else
      _ ->
        {:keep_state, data}
    end
  end

  def leader(:cast, :step_down, data) do
    Logger.info("stepping down", logger_metadata(data))

    {:next_state, :lonely, data, []}
  end

  def leader(:cast, {:user_command, {caller_pid, _ref} = id, {:transfer_leadership, to_node}}, data) do
    if Members.can_vote?(data.members, to_node) do
      data = LeaderState.transfer_leadership(data, to_node, id)
      actions = [{{:timeout, :leadership_transfer_failed}, @leadership_transfer_timeout, :user_requested}]

      RPC.append_entries(data)

      {:keep_state, data, actions}
    else
      send(caller_pid, {id, {:error, :not_voting_member}})

      :keep_state_and_data
    end
  end

  def leader(:cast, {:machine_command, {caller_pid, _ref} = id, _command}, %State{mode_state: %LeaderState{leadership_transfer: %LeadershipTransfer{} = leadership_transfer}}) do
    send(caller_pid, {id, {:error, {:leadership_transfer_in_progress, leadership_transfer.current_candidate}}})

    :keep_state_and_data
  end

  def leader(:cast, {:machine_command, id, command}, data) do
    entry = %CommandEntry{term: data.current_term, command: command}
    log = Log.append(data.log, entry)

    entry_index = Log.latest_index(log)
    client_requests = Map.put(data.mode_state.client_requests, entry_index, id)

    {:keep_state, %State{data | log: log, mode_state: %LeaderState{data.mode_state | client_requests: client_requests}}}
  end

  def leader({:call, from}, _msg, %State{mode_state: %LeaderState{leadership_transfer: %LeadershipTransfer{} = leadership_transfer}}) do
    {:keep_state_and_data, [{:reply, from, {:error, {:leadership_transfer_in_progress, leadership_transfer.current_candidate}}}]}
  end

  def leader({:call, from}, :configuration, data) do
    {:ok, machine_module} = Machine.module(data)

    config = %{
      members: data.members,
      machine_module: machine_module,
      log_module: data.log.module
    }

    {:keep_state_and_data, [{:reply, from, {:ok, config}}]}
  end

  def leader({:call, from}, {:add_member, node}, data) do
    if LeaderState.config_change_in_progress?(data) do
      {:keep_state_and_data, [{:reply, from, {:error, :config_change_in_progress}}]}
    else
      data = LeaderState.add_node(data, node, from, Log.latest_index(data.log) + 1)

      entry =
        %MembershipEntry{
          term: data.current_term,
          members: data.members
        }

      data = %State{data | log: Log.append(data.log, entry)}

      {:keep_state, data}
    end
  end

  def leader({:call, from}, {:remove_member, node}, data) do
    if LeaderState.config_change_in_progress?(data) do
      {:keep_state_and_data, [{:reply, from, {:error, :config_change_in_progress}}]}
    else
      data = LeaderState.remove_node(data, node, from, Log.latest_index(data.log) + 1)

      entry =
        %MembershipEntry{
          term: data.current_term,
          members: data.members
        }

      data = %State{data | log: Log.append(data.log, entry)}

      {:keep_state, data}
    end
  end

  def leader(type, msg, data) do
    Logger.info("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data))

    :keep_state_and_data
  end

  defp become_lonely_timeout do
    {:state_timeout, @lonely_timeout, :become_lonely}
  end

  defp become_follower(%{term: term} = msg, data) do
    Logger.info("received message #{inspect msg} from later term #{term}, becoming/remaining follower", logger_metadata(data))

    {:next_state, :follower, %{data | current_term: term}, [:postpone]}
  end
end
