defmodule Craft.Nexus do
  @moduledoc """
  this module acts as a central hub for cluster messages and member traces

  it implements a "nemesis" which can drop or delay messages programmatically

  additionally, it implements "wait until" functionality, which can block a client process until a cluster condition is met
  """
  use GenServer

  alias Craft.Consensus.State, as: ConsensusState

  defmodule State do
    defstruct [
      members: [],
      term: -1,
      leader: nil,
      log: [],
      # {watcher_from, fun event, private -> :halt | {:cont, private} end, private}
      wait_until: nil,
      # action = :drop | {:delay, msecs} | :forward | {:forward, modified_message}
      # {fn message, private -> {action, private} end, private}
      # {fn message -> action} end, private}
      nemesis: nil
    ]

    def leader_elected(%__MODULE__{term: term} = state, leader, new_term) when new_term > term do
      %__MODULE__{state | leader: leader, term: new_term}
    end

    # trace messages arrived out of order, ignore
    def leader_elected(state, _leader, _new_term), do: state

    def record_event(%__MODULE__{log: log} = state, event) do
      %__MODULE__{state | log: [event | log]}
    end
  end

  def start(members) do
    GenServer.start(__MODULE__, members)
  end

  defdelegate stop(pid), to: GenServer

  def cast(nexus, to, message) do
    GenServer.cast(nexus, {:cast, to, node(), message})
  end

  def wait_until(nexus, condition) do
    GenServer.call(nexus, {:wait_until, condition}, 10_000)
  end

  def nemesis(nexus, fun) when is_function(fun) do
    GenServer.call(nexus, {:nemesis, fun})
  end

  # synchronously sets a nemesis and a wait condition
  def nemesis_and_wait_until(nexus, nemesis, condition) when is_function(nemesis) do
    GenServer.call(nexus, {:nemesis_and_wait_until, nemesis, condition}, 10_000)
  end

  def return_log_and_stop(nexus) do
    GenServer.call(nexus, :return_log_and_stop)
  end

  def init(members) do
    {:ok, %State{members: members}}
  end

  def handle_call({:wait_until, {module, opts}}, from, state) do
    {:noreply, %State{state | wait_until: {from, &module.handle_event/2, module.init(state, opts)}}}
  end

  def handle_call({:wait_until, fun}, from, state) do
    {:noreply, %State{state | wait_until: {from, fun, nil}}}
  end

  def handle_call({:nemesis, fun}, _from, state) do
    {:reply, :ok, %State{state | nemesis: {fun, nil}}}
  end

  def handle_call({:nemesis_and_wait_until, fun, condition}, from, state) do
    {:reply, :ok, state} = handle_call({:nemesis, fun}, from, state)

    handle_call({:wait_until, condition}, from, state)
  end

  def handle_call(:return_log_and_stop, _, state) do
    {:stop, :normal, {:ok, state.log}, state}
  end

  def handle_cast({:cast, to, from, message}, state) do
    {_, to_node} = to
    event = {:cast, to_node, from, message}

    state =
      state
      |> State.record_event({DateTime.utc_now(), event})
      |> evaluate_waiter(event)

    state =
      case state.nemesis do
        {nemesis, private} ->
          {action, private} =
            case Function.info(nemesis, :arity) do
              {:arity, 1} ->
                action = nemesis.(event)
                {action, nil}

              {:arity, 2} ->
                nemesis.(event, private)
            end

          case action do
            :drop ->
              :noop

            :forward ->
              :gen_statem.cast(to, message)

            {:forward, modified_message} ->
              :gen_statem.cast(to, modified_message)

            {:delay, msecs} ->
              :timer.apply_after(msecs, :gen_statem, :cast, [to, message])
          end

          %State{state | nemesis: {nemesis, private}}

        _ ->
          :gen_statem.cast(to, message)

          state
      end

    {:noreply, state}
  end

  def handle_info({_time, {:trace, from, :leader, :enter, :candidate, %ConsensusState{current_term: current_term}}} = event, state) do
    state =
      state
      |> State.record_event(event)
      |> State.leader_elected(from, current_term)
      |> evaluate_waiter(event)

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp evaluate_waiter(%State{wait_until: nil} = state, _event), do: state

  defp evaluate_waiter(%State{wait_until: {wait_from, fun, private}} = state, event) when is_function(fun, 2) do
    case fun.(event, private) do
      {:cont, private} ->
        %State{state | wait_until: {wait_from, fun, private}}

      :halt ->
        GenServer.reply(wait_from, state)

        %State{state | wait_until: nil}
    end
  end
end
