defmodule Craft.Nexus do
  @moduledoc """
  this module acts as a central hub for cluster messages and member traces
  """
  use GenServer

  alias Craft.Consensus.State, as: ConsensusState

  defmodule State do
    defstruct [
      members: [],
      term: -1,
      leader: nil,
      genstatem_invocations: [],
      # {_watcher_from = nil, module, private}
      wait_until: nil,
      # action = :drop | {:delay, msecs} | :forward | {:forward, modified_message}
      # fn message, nemesis_state -> {action, nemesis_state} end
      nemesis: nil,
      nemesis_state: nil
    ]

    def leader_elected(%State{term: term} = state, leader, new_term) when new_term > term do
      %__MODULE__{state | leader: leader, term: new_term}
    end
  end

  def start_link(members) do
    GenServer.start_link(__MODULE__, members)
  end

  defdelegate stop(pid), to: GenServer

  def cast(nexus, to, message) do
    GenServer.cast(nexus, {:cast, to, node(), message})
  end

  def wait_until(nexus, {module, opts}) do
    GenServer.call(nexus, {:wait_until, {module, opts}}, 10_000)
  end

  def set_nemesis(nexus, fun) when is_function(fun, 2) do
    GenServer.call(nexus, {:set_nemesis, fun})
  end

  # synchronously sets a nemesis and a wait condition
  def set_nemesis_and_wait_until(nexus, private, fun) when is_function(fun, 2) do
    GenServer.call(nexus, {:set_nemesis_and_wait_until, private, fun}, 10_000)
  end


  def init(members) do
    {:ok, %State{members: members}}
  end

  def handle_call({:wait_until, {module, opts}}, from, state) do
    {:noreply, %State{state | wait_until: {from, module, module.init(state, opts)}}}
  end

  def handle_call({:set_nemesis, fun}, _from, state) do
    {:reply, :ok, %State{state | nemesis: fun}}
  end

  # def handle_call({:set_nemesis_and_wait_until, private, fun}, from, state) do
  #   {:reply, :ok, %State{state | nemesis_wait_until: {from, fun, private}}}
  # end

  def handle_cast({:cast, to, from, message}, state) do
    {_, to_node} = to
    event = {:cast, to_node, from, message}

    state = evaluate_waiter(state, event)

    :gen_statem.cast(to, message)
    {:noreply, state}

    # cond do
    #   state.nemesis_wait_until ->
    #     {wait_from, nemesis_wait_until, private} = state.nemesis_wait_until
    #     {nemesis_action, wait_action, private} = nemesis_wait_until.(event, private)

    #     case nemesis_action do
    #       :drop ->
    #         :noop

    #       :forward ->
    #         :gen_statem.cast(to, message)

    #       {:forward, modified_message} ->
    #         :gen_statem.cast(to, modified_message)

    #       {:delay, msecs} ->
    #         :timer.apply_after(msecs, :gen_statem, :cast, [to, message])
    #     end

    #     case wait_action do
    #       {:halt, value} ->
    #         GenServer.reply(wait_from, {value, state})
    #         {:noreply, %State{state | nemesis_wait_until: nil}}

    #       :cont ->
    #         {:noreply, %State{state | nemesis_wait_until: {wait_from, nemesis_wait_until, private}}}
    #     end

    #   true ->
    #     :gen_statem.cast(to, message)

    #     {:noreply, state}
    # end

    # {action, nemesis_state} = state.nemesis.(event, state.nemesis_state)


    # state =
    #   case message do
    #     %AppendEntries{} = append_entries ->
    #       {_, member} = to

    #       State.append_entries_received(state, member, append_entries)

    #     %AppendEntries.Results{} = append_entries_results ->
    #       {_, leader} = to
    #       state = State.append_entries_results_received(state, leader, append_entries_results)

    #       case state.wait_until do
    #         {watcher, :all_stable} ->
    #           if State.all_stable?(state) do
    #             GenServer.reply(watcher, state)

    #             %State{state | wait_until: nil}
    #           else
    #             state
    #           end

    #         {watcher, :majority_stable} ->
    #           if State.majority_stable?(state) do
    #             GenServer.reply(watcher, state)

    #             %State{state | wait_until: nil}
    #           else
    #             state
    #           end

    #         _ ->
    #           state
    #       end

    #     _ ->
    #       state
    #   end

    # {:noreply,  %State{state | nemesis_state: nemesis_state}}
  end

  def handle_info({:trace, _time, from, :leader, :enter, :candidate, %ConsensusState{current_term: current_term}} = event, state) do
    state =
      state
      |> State.leader_elected(from, current_term)
      |> evaluate_waiter(event)

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp evaluate_waiter(%State{wait_until: nil = state}, _event), do: state

  defp evaluate_waiter(%State{wait_until: {wait_from, module, private}} = state, event) do
    {wait_action, private} = module.handle_event(event, private)

    case wait_action do
      :cont ->
        %State{state | wait_until: {wait_from, module, private}}

      :halt ->
        GenServer.reply(wait_from, state)

        %State{state | wait_until: nil}
    end
  end

  # # @spec watch(member_nodes, until :: {:millisecs, pos_integer()}) | :group_stable
  # def watch(members, until \\ :group_stable)
  # def watch(members, millisecs: millisecs) do
  #   Process.send_after(self(), :stop_watching, millisecs)

  #   members |> State.new(millisecs) |> do_watch()
  # end
  # def watch(members, until), do: members |> State.new(until) |> do_watch()

  # defp do_watch(state) do
  #   receive do
  #     :stop_watching ->
  #       invocations = Enum.sort_by(state.genstatem_invocations, fn {time, _} -> time end)
  #       {:ok, %State{state | genstatem_invocations: invocations}}

  #     {:cast, _time, from, to, msg} ->
  #       case handle_cast(from, to, msg, state) do
  #         %State{} = state ->
  #           :gen_statem.cast(to, msg)

  #           do_watch(state)

  #         result ->
  #           result
  #       end

  #     {:trace, _time, from, :leader, :enter, :candidate, %CandidateState{current_term: current_term}} ->
  #       state
  #       # |> record_invocation(invocation)
  #       |> State.leader_elected(from, current_term)
  #       |> do_watch()

  #     {:trace, _, _, _, _} ->
  #       state
  #       # |> record_invocation(invocation)
  #       |> do_watch()
  #   end
  # end

  # defp handle_cast(_from, {_, to_node}, %AppendEntries{} = append_entries, %State{until: :group_stable} = state) do
  #   State.append_entries_received(state, to_node, append_entries)
  # end

  # defp handle_cast(_from, {_, to_node}, %AppendEntries.Results{} = append_entries_results, %State{until: :group_stable} = state) do
  #   state = State.append_entries_results_received(state, to_node, append_entries_results)

  #   if State.group_stable?(state) do
  #     {:ok, :group_stable, state}
  #   else
  #     state
  #   end
  # end

  # defp handle_cast(_, _, _, state), do: state

  # # defp record_invocation(state, invocation) do
  # #   invocation = {DateTime.utc_now(), invocation}
  # #   %State{state | genstatem_invocations: [invocation | state.genstatem_invocations]}
  # # end
end
