defmodule Craft.Consensus do
  @moduledoc false

  #              |
  #              |
  #              |
  #              v
  # --------> lonely ------> follower -> lonely -> candidate -> leader
  # |                      |
  # |                      |
  # L- receiving_snapshot <-
  #
  # receiving_snapshot: receiving snapshot from the (possibly no longer) leader, never votes
  #                     won't leave this state until snapshot transfer completes
  # follower: happily following leader
  # lonely: hasn't heard from leader in a while (or ever), would be willing to vote 'yes' in a non-transfer election
  # candidate: follower that's called an election after a successful majority pre-vote
  # leader: candidate that's won majority vote

  alias Craft.Consensus.State
  alias Craft.Consensus.State.LeaderState
  alias Craft.Consensus.State.LeaderState.LeadershipTransfer
  alias Craft.Consensus.State.LeaderState.MembershipChange
  alias Craft.Consensus.State.Members
  alias Craft.GlobalTimestamp
  alias Craft.Log.CommandEntry
  alias Craft.Log.MembershipEntry
  alias Craft.Machine
  alias Craft.MemberCache
  alias Craft.Persistence
  alias Craft.RPC
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.InstallSnapshot
  alias Craft.RPC.RequestVote
  alias Craft.SnapshotServerClient

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1, logger_metadata: 2]
  import Craft.Application, only: [via: 2]

  @behaviour :gen_statem

  @heartbeat_interval 100 # ms

  # max time in the past within which leader must have a successful quorum, or it'll step down
  @checkquorum_interval @heartbeat_interval * 3

  @lonely_timeout @heartbeat_interval + 1_000

  # setting the leader lease length to something a bit less than the lonely timeout ensures that the new leader
  # can pick up the lease immediately after it's elected. this is probably what you want, it increses availability
  # in split-brain scenarios (this is what TiKV does).
  #
  # you don't have to do this, you're free to set the value however you like, the new leader will just wait out the old lease.
  @leader_lease_period @lonely_timeout - 200

  # amount of time to wait for votes before concluding that the election has failed
  @election_timeout 1500
  # max jitter for lonely election initiation
  @election_timeout_jitter 1500

  # amount of time to wait for new leader to take over before concluding that leadership transfer has failed
  @leadership_transfer_timeout 3000

  #
  # API
  #

  def command(name, command) do
    :gen_statem.call(via(name, __MODULE__), {:machine_command, command})
  end

  def cast_user_command(name, node, msg, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    id = {self(), make_ref()}

    remote_operation(name, node, :cast, {:user_command, id, msg})

    receive do
      {^id, reply} ->
        reply

      after
        timeout ->
          {:error, :timeout}
    end
  end

  def state(name, node) do
    remote_operation(name, node, :call, :state)
  end

  def configuration(name, node) do
    remote_operation(name, node, :call, :configuration)
  end

  def add_member(name, node, member) do
    remote_operation(name, node, :call, {:add_member, member})
  end

  def remove_member(name, node, member) do
    remote_operation(name, node, :call, {:remove_member, member})
  end

  def transfer_leadership(name, node, to_node) do
    cast_user_command(name, node, {:transfer_leadership, to_node})
  end

  def transfer_leadership(name, node) do
    cast_user_command(name, node, :transfer_leadership)
  end

  def step_down(name, node) do
    remote_operation(name, node, :cast, :step_down)
  end

  def snapshot_ready(name, index, path) do
    :gen_statem.call(via(name, __MODULE__), {:snapshot_ready, index, path})
  end

  # we can't use the {name, node} form, since `name` must be an atom, and we allow anything to be a group name
  # so we use the component registry on the remote node
  def remote_operation(name, node, operation, msg) do
    :rpc.call(node, __MODULE__, :do_operation, [operation, name, msg])
  end
  def do_operation(:cast, name, msg), do: :gen_statem.cast(via(name, __MODULE__), msg)
  def do_operation(:call, name, msg), do: :gen_statem.call(via(name, __MODULE__), msg)

  #
  # genstatem implementation
  #

  def callback_mode, do: [:state_functions, :state_enter]

  def start_link(args) do
    :gen_statem.start_link(via(args.name, __MODULE__), __MODULE__, args, [])
  end

  def init(args) do
    Logger.metadata(name: args.name, node: node(), nexus: args[:nexus_pid])

    data = State.new(args.name, args[:nodes], args.persistence, args.machine, args[:global_clock], args[:nexus_pid])

    if nexus_pid = args[:nexus_pid] do
      remote_group_leader = :rpc.call(node(nexus_pid), Process, :whereis, [:init])
      :logger.update_process_metadata(%{gl: remote_group_leader})
    end

    if args[:manual_start] do
      {:ok, :waiting_to_start, data}
    else
      {:ok, :lonely, continue_init(data)}
    end
  end

  defp continue_init(data) do
    MemberCache.update(data)

    {:ok, snapshot} = Machine.init_or_restore(data)

    if data.global_clock do
      Logger.info("consensus process started, global clock present, leader leases enabled", logger_metadata(data))
    else
      Logger.info("consensus process started, no global clock present, leader leases disabled", logger_metadata(data))
    end

    %{data | snapshot: snapshot}
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  #
  # manual start (test only)
  #

  if Mix.env() == :test do
    def waiting_to_start(:enter, _, _data), do: :keep_state_and_data
    def waiting_to_start(:cast, :run, data) do
      data = continue_init(data)

      {:next_state, data.state, data, []}
    end
    # def ready_to_test({:call, _from}, :catch_up, _data), do: {:keep_state_and_data, [:postpone]}
  end

  #
  # Lonely
  #
  # hasn't heard from a leader in a while, will:
  #
  # - vote for candidates
  # - hold pre-vote elections as a precursor to becoming a candidate for a true election
  #

  def lonely(:enter, _previous_state, data) do
    data = State.become_lonely(data)

    MemberCache.update(data)

    Machine.update_role(data)

    Logger.info("became lonely", logger_metadata(data, trace: {:became, :lonely}))

    {:keep_state, data, [{:state_timeout, :rand.uniform(@election_timeout_jitter), :begin_pre_vote}]}
  end

  def lonely(:state_timeout, :begin_pre_vote, data) do
    Logger.info("pre-vote started", logger_metadata(data, trace: :pre_vote_started))

    RPC.request_vote(data, pre_vote: true)

    {:keep_state_and_data, [{:state_timeout, @election_timeout, :election_failed}]}
  end

  def lonely(:state_timeout, :election_failed, data) do
    Logger.info("pre-vote failed, repeating state", logger_metadata(data, trace: :pre_vote_failed))

    :repeat_state_and_data
  end

  # if we receive a message with a higher term, bump our term and process the message
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def lonely(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term do
    lonely(:cast, msg, State.become_lonely(data, term))
  end

  def lonely(:cast, %RequestVote{pre_vote: true} = request_vote, data) do
    vote_granted = State.vote_for?(data, request_vote)

    Logger.debug("#{if vote_granted, do: "granting", else: "denying"} pre-vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: vote_granted}))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  def lonely(:cast, %RequestVote{pre_vote: false} = request_vote, %State{voted_for: nil} = data) do
    {vote_granted, data} =
      if State.vote_for?(data, request_vote) do
        {true, State.set_current_term(data, data.current_term, request_vote.candidate_id)}
      else
        {false, data}
      end

    Logger.debug("#{if vote_granted, do: "granting", else: "denying"} vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: vote_granted}))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  def lonely(:cast, %RequestVote{pre_vote: false} = request_vote, data) do
    Logger.debug("denying vote to #{request_vote.candidate_id}, already voted in this term", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: false}))

    RPC.respond_vote(request_vote, false, data)

    {:keep_state, data}
  end

  def lonely(:cast, %RequestVote.Results{pre_vote: true} = results, data) do
    Logger.debug("pre-vote #{if results.vote_granted, do: "granted", else: "denied"} by #{results.from}", logger_metadata(data, trace: {:vote_received, granted?: results.vote_granted, pre_vote?: true}))

    data = State.record_vote(data, results)

    case State.election_result(data) do
      :won ->
        Logger.info("won pre-vote election", logger_metadata(data, trace: :won_pre_vote_election))

        {:next_state, :candidate, data}

      :lost ->
        Logger.info("lost pre-vote election", logger_metadata(data, trace: :lost_pre_vote_election))

        {:repeat_state, data}

      :pending ->
        {:keep_state, data}
    end
  end

  def lonely(:cast, %AppendEntries{}, data) do
    {:next_state, :follower, data, [:postpone]}
  end

  def lonely(:cast, %InstallSnapshot{}, data) do
    {:next_state, :receiving_snapshot, data, [:postpone]}
  end

  def lonely(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, not_leader_response(data)})

    :keep_state_and_data
  end

  def lonely({:call, from}, :state, data) do
    {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
  end

  def lonely({:call, from}, {:snapshot_ready, index, path}, data) do
    data = State.snapshot_ready(data, index, path)

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def lonely({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, not_leader_response(data)}]}
  end

  def lonely(type, msg, data) do
    Logger.debug("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  #
  # Receiving Snapshot
  #
  # being sent a snapshot from the (possibly unseated) leader.
  #
  # TODO: consider case where a follower is receiving a snapshot and an election occurs, the new leader may have truncated its log to the point where
  #       the snapshot that's currently being transferred from an the leader may be abandoned and re-started from the current leader. the new leader
  #       wouldn't have the log gap between the snapshot point and the leader's oldest non-compacted log entry.
  #

  def receiving_snapshot(:enter, _previous_state, data) do
    data = State.become_receiving_snapshot(data)

    Machine.update_role(data)

    Logger.info("became receiving_snapshot follower", logger_metadata(data, event: {:became, :receiving_snapshot}))

    {:keep_state, data, []}
  end

  def receiving_snapshot(:cast, %InstallSnapshot{snapshot_transfer: s} = install_snapshot, %State{incoming_snapshot_transfer: {_pid, s}} = data) do
    Logger.debug("ignoring #{inspect install_snapshot.__struct__} message", logger_metadata(data, trace: {:ignored_msg, install_snapshot}))

    data =
      %{data | leader_id: install_snapshot.leader_id}
      |> State.set_current_term(install_snapshot.term)

    MemberCache.update(data)

    {:keep_state, data}
  end

  def receiving_snapshot(:cast, %InstallSnapshot{} = install_snapshot, %State{} = data) do
    Logger.debug("receiving snapshot", logger_metadata(data, trace: {:receiving_snapshot, install_snapshot}))

    data =
      %{data | leader_id: install_snapshot.leader_id}
      |> State.set_current_term(install_snapshot.term)

    MemberCache.update(data)

    if data.machine.__craft_mutable__() do
      case data.incoming_snapshot_transfer do
        {_old_pid, _old_transfer} ->
          # FIXME: abort current transfer
          :abort

        nil ->
          :noop
      end

      {:ok, data_dir} = Machine.prepare_to_receive_snapshot(data.name)

      me = self()
      {:ok, pid} =
        SnapshotServerClient.start_link(
          install_snapshot.snapshot_transfer,
          data_dir,
          fn
            :ok ->
              :gen_statem.cast(me, {:download_succeeded, install_snapshot})

            error ->
              :gen_statem.cast(me, {:download_failed, error})
          end
        )

      {:keep_state, %{data | incoming_snapshot_transfer: {pid, install_snapshot.snapshot_transfer}}}
    else
      receiving_snapshot(:cast, {:download_succeeded, install_snapshot}, data)
    end
  end

  def receiving_snapshot(:cast, {:download_succeeded, %InstallSnapshot{} = install_snapshot}, %State{} = data) do
    Logger.debug("snapshot download succeeded", logger_metadata(data, trace: :snapshot_download_succeeded))

    persistence = Persistence.truncate(data.persistence, install_snapshot.log_index, install_snapshot.log_entry)

    :ok = Machine.receive_snapshot(data.name, install_snapshot)

    RPC.respond_install_snapshot(install_snapshot, true, data)

    {:next_state, :follower, %{data | persistence: persistence}}
  end

  def receiving_snapshot(:cast, {:download_failed, reason}, %State{} = data) do
    Logger.error("error receiving snapshot because: #{inspect reason}, retrying.", logger_metadata(data, trace: {:error_receiving_snapshot, reason}))

    #TODO: delay?

    {:repeat_state, %{data | incoming_snapshot_transfer: nil}}
  end

  def receiving_snapshot(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, not_leader_response(data)})

    :keep_state_and_data
  end

  def receiving_snapshot({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, not_leader_response(data)}]}
  end

  #
  # Follower
  #
  # hears heartbeats from the leader, appends log entries
  #

  def follower(:enter, _previous_state, data) do
    data = State.become_follower(data)

    MemberCache.update(data)

    Machine.update_role(data)

    Logger.info("became follower", logger_metadata(data, trace: {:became, :follower}))

    {:keep_state, data, [become_lonely_timeout()]}
  end

  def follower(:state_timeout, :become_lonely, data), do: {:next_state, :lonely, data}

  # we just installed this snapshot, the leader hasn't heard yet, ignore it.
  def follower(:cast, %InstallSnapshot{snapshot_transfer: s}, %State{incoming_snapshot_transfer: {_pid, s}}) do
    :keep_state_and_data
  end

  def follower(:cast, %RequestVote{leadership_transfer: true, term: term} = request_vote, %State{current_term: current_term} = data) when term > current_term do
    {vote_granted, data} =
      if State.vote_for?(data, request_vote) do
        {true, State.set_current_term(data, term, request_vote.candidate_id)}
      else
        {false, data}
      end

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} leadership transfer vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: vote_granted}))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  # followers are happy with the leader, they vote "no" to all non-transfer elections types, regardless of term
  def follower(:cast, %RequestVote{} = request_vote, data) do
    Logger.info("denying #{if request_vote.pre_vote, do: "pre-", else: ""}vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: false, reason: "not lonely"}))

    RPC.respond_vote(request_vote, false, data)

    :keep_state_and_data
  end

  # if we receive an AppendEntries message with a higher term, bump our term and process the message
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def follower(:cast, %AppendEntries{term: term} = msg, %State{current_term: current_term} = data) when term > current_term do
    follower(:cast, msg, State.set_current_term(data, term))
  end

  # TODO: move most of this into State module?
  def follower(:cast, %AppendEntries{} = append_entries, data) do
    prev_log_term = append_entries.prev_log_term
    old_commit_index = data.commit_index
    data =
      %{data | leader_id: append_entries.leader_id}
      |> State.set_lease_expires_at(append_entries.lease_expires_at)

    {success, data} =
      if Enum.empty?(append_entries.entries) do
        {true, data}
      else
        case Persistence.fetch(data.persistence, append_entries.prev_log_index) do
          {:ok, %{term: ^prev_log_term}} ->
            rewound_entries = Persistence.fetch_from(data.persistence, append_entries.prev_log_index + 1)

            persistence =
              data.persistence
              |> Persistence.rewind(append_entries.prev_log_index)
              |> Persistence.append(append_entries.entries)

            data = %{data | persistence: persistence}

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
                {true, %{data | members: members}}

              # if the entries that we've rewound contained a membership entry, and the incoming entries from the
              # leader don't include a new membership entry, we need to look back through the log until we find one
              # to determine the current cluster membership (section 4.1)
              nil ->
                Enum.find_value(rewound_entries, {true, data}, fn
                  %MembershipEntry{members: members} ->
                    {true, %{data | members: members}}

                  _ ->
                    false
                end)
            end

          _ ->
            {false, %{data | persistence: Persistence.rewind(data.persistence, append_entries.prev_log_index - 1)}}
        end
      end

    data = %{data | commit_index: min(append_entries.leader_commit, Persistence.latest_index(data.persistence))}

    Logger.debug("leader heartbeat from #{append_entries.leader_id}, restarting timer", logger_metadata(data))

    RPC.respond_append_entries(append_entries, success, data)

    if success && data.commit_index > old_commit_index do
      # TODO: make log length configurable
      log_too_long = Persistence.length(data.persistence) > 20

      Logger.debug("quorum reached", logger_metadata(data, trace: :quorum_reached))

      Machine.quorum_reached(data, log_too_long)
    end

    MemberCache.update(data)

    # leader told us to take over leadership if our log is caught up
    if append_entries.leadership_transfer &&
       append_entries.leadership_transfer.latest_index == Persistence.latest_index(data.persistence) &&
       append_entries.leadership_transfer.latest_term == Persistence.latest_term(data.persistence) do
      {:next_state, :candidate, %{data | leadership_transfer_request_id: append_entries.leadership_transfer.from}}
    else
      {:keep_state, data, [become_lonely_timeout()]}
    end
  end

  def follower(:cast, %InstallSnapshot{}, data) do
    {:next_state, :receiving_snapshot, data, [:postpone]}
  end

  def follower(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, not_leader_response(data)})

    :keep_state_and_data
  end

  def follower({:call, from}, :state, data) do
    {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
  end

  def follower({:call, from}, {:snapshot_ready, index, path_or_content}, data) do
    data = State.snapshot_ready(data, index, path_or_content)

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def follower({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, not_leader_response(data)}]}
  end

  def follower(type, msg, data) do
    Logger.debug("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  #
  # Candidate
  #

  def candidate(:enter, :follower, %State{leadership_transfer_request_id: id} = data) when is_tuple(id) or id == :internal do
    data = State.become_candidate(data)

    MemberCache.update(data)

    Machine.update_role(data)

    Logger.info("became candidate, initiating leadership transfer election", logger_metadata(data, trace: {:became, :candidate}))

    RPC.request_vote(data, leadership_transfer: true)

    # TODO: if election fails, send :error to leadership_transfer_request_id
    {:keep_state, data, [{:state_timeout, @election_timeout, :election_failed}]}
  end

  def candidate(:enter, _previous_state, data) do
    data = State.become_candidate(data)

    MemberCache.update(data)

    Machine.update_role(data)

    Logger.info("became candidate", logger_metadata(data, trace: {:became, :candidate}))

    RPC.request_vote(data)

    {:keep_state, data, [{:state_timeout, @election_timeout, :election_failed}]}
  end

  def candidate(:state_timeout, :election_failed, data) do
    Logger.info("election failed, becoming lonely", logger_metadata(data, trace: :election_failed))

    {:next_state, :lonely, data}
  end

  # ignore messages from earlier terms
  def candidate(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.debug("ignoring #{inspect msg.__struct__} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def candidate(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term, do: become_lonely(msg, data)

  # refuse to vote for another candidate
  def candidate(:cast, %RequestVote{} = request_vote, data) do
    Logger.info("denying #{(if request_vote.pre_vote, do: "pre-", else: "")}vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_received, granted?: false, pre_vote?: request_vote.pre_vote}))

    RPC.respond_vote(request_vote, false, data)

    :keep_state_and_data
  end

  # if a competing candidate becomes leader, convert to follower and process the AppendEntries
  def candidate(:cast, %AppendEntries{}, data) do
    {:next_state, :follower, data, [:postpone]}
  end

  def candidate(:cast, %RequestVote.Results{} = results, data) do
    if results.vote_granted do
      Logger.info("vote granted by #{results.from}", logger_metadata(data, trace: {:vote_received, granted?: results.vote_granted}))
    else
      Logger.info("vote denied by #{results.from}", logger_metadata(data, trace: {:vote_received, granted?: results.vote_granted}))
    end

    # keep the future-most lease timestamp
    latest_leader_lease =
      case {data.lease_expires_at, results.lease_expires_at} do
        {nil, nil} ->
          nil

        {nil, %GlobalTimestamp{} = follower_ts} ->
          follower_ts

        {%GlobalTimestamp{} = our_ts, nil} ->
          our_ts

        {%GlobalTimestamp{} = our_ts, %GlobalTimestamp{} = follower_ts} ->
          if DateTime.compare(our_ts.latest, follower_ts.latest) == :lt do
            follower_ts
          else
            our_ts
          end
      end

    data =
      data
      |> State.set_lease_expires_at(latest_leader_lease)
      |> State.record_vote(results)

    case State.election_result(data) do
      :won ->
        Logger.info("election won, becoming leader", logger_metadata(data, trace: :election_won))
        {:next_state, :leader, data}

      :lost ->
        Logger.info("election lost, becoming lonely", logger_metadata(data, trace: :election_lost))
        {:next_state, :lonely, data}

      :pending ->
        {:keep_state, data}
    end
  end

  # even though this node doesn't recognize the leader as legitimate anymore,
  # we should still try to redirect commands to the old leader, in case it
  # actually is legitimate and we're incorrect. (e.g. we're isolated from the
  # other nodes and they're happily carrying on without us)
  def candidate(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, not_leader_response(data)})

    :keep_state_and_data
  end

  def candidate({:call, from}, :state, data) do
    {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
  end

  def candidate({:call, from}, {:snapshot_ready, index, path_or_content}, data) do
    data = State.snapshot_ready(data, index, path_or_content)

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def candidate({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, not_leader_response(data)}]}
  end

  def candidate(type, msg, data) do
    Logger.debug("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  #
  # Leader
  #

  def leader(:enter, previous_state, %State{leadership_transfer_request_id: {caller_pid, _ref} = id} = data) do
    send(caller_pid, {id, :ok})

    leader(:enter, previous_state, %{data | leadership_transfer_request_id: nil})
  end

  def leader(:enter, _previous_state, data) do
    data = State.become_leader(data)

    MemberCache.update(data)

    Machine.update_role(data)

    #
    # when the cluster starts up, each node is explicitly given the same configuration to
    # hold in-memory to bootstrap the cluster.
    #
    # however, we need to store the current config in the log so that when nodes restart,
    # they have a way to determine what the current config is (walk backwards from the end of the
    # log looking for the most recent config)
    #
    # appending an entry from the current term also allows us to commit any possibly uncommitted entries
    # from previous terms (section 5.4.2)
    #
    # even if we don't hold the lease, we still proceed with committing this no-op entry, and hence with committing
    # previous terms' entries and forcing followers logs to conform to ours. i believe this is safe, because (by design)
    # the new leader has no idea who asked for what writes to occur, that information is stored emphemerally by the
    # leader's machine and not in the log itself. so any hanging writes (section 5.4.2 operations) that were requested of the
    # previous leader can not be answered by the new leader. since those writes will never be confirmed to the client,
    # they're not "witnessed" state confirmations according to the outside world. and hence they're not a linearizability violation.
    #
    data = %{data | persistence: Persistence.append(data.persistence, MembershipEntry.new(data))}

    actions = [
      {:state_timeout, 0, :heartbeat},
      {{:timeout, :check_quorum}, @checkquorum_interval, :check_quorum}
    ]

    if data.global_clock do
      if data.lease_expires_at do
        actions = [{{:timeout, :takeover}, 0, data.lease_expires_at} | actions]
        data = put_in(data.leader_state.waiting_for_lease, true)

        Logger.info("became leader, waiting for lease", logger_metadata(data, trace: {:became, :leader}))

        {:keep_state, data, actions}
      else
        Logger.info("became leader, immediately taking lease", logger_metadata(data, trace: {:became, :leader}))

        {:keep_state, data, actions}
      end
    else
      Logger.info("became leader", logger_metadata(data, trace: {:became, :leader}))

      {:keep_state, data, actions}
    end
  end

  def leader(:state_timeout, :heartbeat, data) do
    {:keep_state, heartbeat(data), [{:state_timeout, @heartbeat_interval, :heartbeat}]}
  end

  def leader({:timeout, :check_quorum}, :check_quorum, data) do
    if data.leader_state.last_quorum_at < :erlang.monotonic_time(:millisecond) - @checkquorum_interval do
      Logger.info("unable to make quorum, stepping down.", logger_metadata(data, trace: {:check_quorum, :failed}))

      {:next_state, :lonely, data}
    else
      Logger.debug("check-quorum successful", logger_metadata(data, trace: {:check_quorum, :ok}))

      {:keep_state_and_data, [{{:timeout, :check_quorum}, @checkquorum_interval, :check_quorum}]}
    end
  end

  def leader({:timeout, :takeover}, old_lease_expires_at, data) do
    # don't assume that because our lease wait-out event fired that the lease has expired,
    # our monotonic clock could be wrong, the user-provided clock source is the authority
      case GlobalTimestamp.time_until_lease_expires(data.global_clock, old_lease_expires_at) do
        {:ok, 0} ->
          Logger.info("taking over lease", logger_metadata(data, trace: :lease_takeover))

          {:keep_state, put_in(data.leader_state.waiting_for_lease, false)}

        {:ok, wait_time} ->
          Logger.debug("waiting #{wait_time}ms for lease", logger_metadata(data, trace: {:waiting_out_lease, wait_time}))

          {:keep_state_and_data, [{{:timeout, :takeover}, wait_time, old_lease_expires_at}]}

        error ->
          Logger.warning("unable to acquire lease, global clock error: #{inspect error}, becoming follower", logger_metadata(data, trace: :global_clock_failure))

          {:next_state, :lonely, data}
      end
  end

  # ignore any messages from earlier terms
  def leader(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.debug("ignoring #{inspect msg.__struct__} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def leader(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term, do: become_lonely(msg, data)

  def leader(:cast, %RequestVote{pre_vote: true} = request_vote, data) do
    vote_granted = State.vote_for?(data, request_vote)

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} pre-vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: vote_granted}))

    RPC.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  # ignore superfluous votes from when we were a candidate
  def leader(:cast, %RequestVote.Results{}, _data), do: :keep_state_and_data

  def leader(:cast, %AppendEntries.Results{} = results, data) do
    case LeaderState.handle_append_entries_results(data, results) do
      {:needs_snapshot, data} ->
        RPC.install_snapshot(data, results.from)

        {:keep_state, data}

      data ->
        data =
          Enum.reduce(data.members.catching_up_nodes, data, fn node, data ->
            if Persistence.latest_index(data.persistence) <= Map.get(data.leader_state.match_indices, node) do
              Logger.info("node #{inspect node} is caught up", logger_metadata(data))

              data = %{data | members: Members.allow_node_to_vote(data.members, node)}

              %{data | persistence: Persistence.append(data.persistence, MembershipEntry.new(data))}
            else
              data
            end
          end)

        # the membership change has committed
        with %MembershipChange{} = membership_change <- data.leader_state.membership_change,
             true <- data.commit_index >= membership_change.log_index do
          data = put_in(data.leader_state.membership_change, nil)
          actions = [{:reply, membership_change.from, :ok}]

          # if we're being removed, transfer leadership away first
          if membership_change.action == :remove && membership_change.node == node() do
            data = LeaderState.transfer_leadership(data)

            actions = [{{:timeout, :leadership_transfer_failed}, @leadership_transfer_timeout, :self_removal} | actions]

            {:keep_state, heartbeat(data), actions}
          else
            {:keep_state, data, actions}
          end
        else
          _ ->
            {:keep_state, data}
        end
    end
  end

  def leader(:cast, %InstallSnapshot.Results{} = results, data) do
    {:keep_state, LeaderState.handle_install_snapshot_results(data, results)}
  end

  def leader(:cast, :step_down, data) do
    Logger.info("stepping down", logger_metadata(data, trace: :step_down))

    {:next_state, :lonely, data, []}
  end

  def leader(:cast, {:user_command, id, :transfer_leadership}, data) do
    to_node =
      data.members
      |> Members.other_voting_nodes()
      |> Enum.random()

    data = LeaderState.transfer_leadership(data, to_node, id)

    actions = [{{:timeout, :leadership_transfer_failed}, @leadership_transfer_timeout, :user_requested}]

    {:keep_state, heartbeat(data), actions}
  end

  # user asked to transfer leadership to existing leader, noop
  def leader(:cast, {:user_command, {caller_pid, _ref} = id, {:transfer_leadership, to_node}}, _data) when to_node == node() do
    send(caller_pid, {id, :ok})

    :keep_state_and_data
  end

  def leader(:cast, {:user_command, {caller_pid, _ref} = id, {:transfer_leadership, to_node}}, data) do
    if Members.can_vote?(data.members, to_node) do
      data = LeaderState.transfer_leadership(data, to_node, id)

      actions = [{{:timeout, :leadership_transfer_failed}, @leadership_transfer_timeout, :user_requested}]

      {:keep_state, heartbeat(data), actions}
    else
      send(caller_pid, {id, {:error, :not_voting_member}})

      :keep_state_and_data
    end
  end

  def leader({:call, from}, {:machine_command, _command}, %State{leader_state: %LeaderState{leadership_transfer: %LeadershipTransfer{} = leadership_transfer}}) do
    {:keep_state_and_data, [{:reply, from, {:error, {:leadership_transfer_in_progress, leadership_transfer.current_candidate}}}]}
  end

  def leader({:call, from}, {:machine_command, command}, %State{global_clock: global_clock} = data) when not is_nil(global_clock) do
    case GlobalTimestamp.time_until_lease_expires(data.global_clock, data.lease_expires_at) do
      {:ok, time} when time > 0 ->
        append_command(command, from, data)

      {:ok, 0} ->
        {:keep_state_and_data, [{:reply, from, {:error, :not_leaseholder}}]}

      error ->
        Logger.error("unable to determine global time for command, got #{inspect error}, becoming follower", logger_metadata(data, trace: :global_clock_failure))

        {:next_state, :lonely, data, [{:reply, from, {:error, :not_leaseholder}}]}
    end
  end

  def leader({:call, from}, {:machine_command, command}, data) do
    append_command(command, from, data)
  end

  def leader({:call, from}, {:snapshot_ready, index, path_or_content}, data) do
    data = State.snapshot_ready(data, index, path_or_content)

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def leader({:call, from}, _msg, %State{leader_state: %LeaderState{leadership_transfer: %LeadershipTransfer{} = leadership_transfer}}) do
    {:keep_state_and_data, [{:reply, from, {:error, {:leadership_transfer_in_progress, leadership_transfer.current_candidate}}}]}
  end

  def leader({:call, from}, :configuration, data) do
    config =
      data
      |> Map.take([:members, :nexus_pid])
      |> Map.merge(%{machine_module: data.machine, persistence_module: data.persistence.module})

    {:keep_state_and_data, [{:reply, from, {:ok, config}}]}
  end

  def leader({:call, from}, {:add_member, node}, data) do
    if LeaderState.config_change_in_progress?(data) do
      {:keep_state_and_data, [{:reply, from, {:error, :config_change_in_progress}}]}
    else
      data = LeaderState.add_node(data, node, from, Persistence.latest_index(data.persistence) + 1)
      entry = %MembershipEntry{term: data.current_term, members: data.members}
      data = %{data | persistence: Persistence.append(data.persistence, entry)}

      MemberCache.update(data)

      {:keep_state, data}
    end
  end

  def leader({:call, from}, {:remove_member, node}, data) do
    if LeaderState.config_change_in_progress?(data) do
      {:keep_state_and_data, [{:reply, from, {:error, :config_change_in_progress}}]}
    else
      data = LeaderState.remove_node(data, node, from, Persistence.latest_index(data.persistence) + 1)
      entry = %MembershipEntry{term: data.current_term, members: data.members}
      data = %{data | persistence: Persistence.append(data.persistence, entry)}

      MemberCache.update(data)

      {:keep_state, data}
    end
  end

  def leader({:call, from}, :state, data) do
    {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
  end

  def leader(type, msg, data) do
    Logger.debug("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  def terminate(_reason, _state, data) do
    Persistence.close(data.persistence)
  end

  defp become_lonely_timeout do
    {:state_timeout, @lonely_timeout, :become_lonely}
  end

  defp become_lonely(%{term: term}, data) do
    {:next_state, :lonely, State.set_current_term(data, term), [:postpone]}
  end

  defp not_leader_response(%State{leader_id: nil}), do: {:error, :unknown_leader}
  defp not_leader_response(%State{leader_id: leader_id}), do: {:error, {:not_leader, leader_id}}

  defp heartbeat(%State{} = state) do
    # the monotonic clock is not strictly increasing, so it can freeze unboundedly.
    # if that happens, we add a millisecond to the last round's value to continue generating unique consensus round ids
    now = :erlang.monotonic_time(:millisecond)
    last_heartbeat_sent_at =
      if state.leader_state.last_heartbeat_sent_at < now do
        now
      else
        state.leader_state.last_heartbeat_sent_at + 1
      end
    state = put_in(state.leader_state.last_heartbeat_sent_at, last_heartbeat_sent_at)
    state = put_in(state.leader_state.current_quorum_successful, false)

    state =
      if state.global_clock do
        case GlobalTimestamp.now(state.global_clock) do
          {:ok, now} ->
            State.set_lease_expires_at(state, GlobalTimestamp.add(now, @leader_lease_period, :millisecond))

          error ->
            Logger.error("unable to determine global time, got #{inspect error}, becoming follower", logger_metadata(state, trace: :global_clock_failure))

            throw({:next_state, :lonely, state})
        end
      else
        state
      end

    Enum.reduce(Members.other_nodes(state.members), state, fn to_node, state ->
      if LeaderState.needs_snapshot?(state, to_node) do
        state =
          if LeaderState.sending_snapshot?(state, to_node) do
            state
          else
            if state.machine.__craft_mutable__() do
              LeaderState.create_snapshot_transfer(state, to_node)
            else
              state
            end
          end

        RPC.install_snapshot(state, to_node)

        state
      else
        RPC.append_entries(state, to_node)

        state
      end
    end)
  end

  defp append_command(command, from, data) do
    entry = %CommandEntry{term: data.current_term, command: command}
    persistence = Persistence.append(data.persistence, entry)
    entry_index = Persistence.latest_index(persistence)

    {:keep_state, %{data | persistence: persistence}, [{:reply, from, {:ok, entry_index}}]}
  end
end
