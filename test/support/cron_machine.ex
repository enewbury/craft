defmodule Craft.CronMachine do
  use Craft.Machine, mutable: false

  require Logger

  def schedule(group_name, mfa, time) do
    Craft.command({:schedule, mfa, time}, group_name)
  end

  defmodule State do
    defstruct [
      :group_name,
      jobs: MapSet.new()
    ]
  end

  @impl true
  def init(group_name) do
    Process.put(:timers, %{})

    {:ok, %State{group_name: group_name}}
  end

  @impl true
  def handle_command({:schedule, mfa, time}, _log_index, state) do
    job = {mfa, time}

    if MapSet.member?(state.jobs, job) do
      {{:error, :already_scheduled}, state}
    else
      start_timer(state.group_name, job)

      {:ok, %{state | jobs: MapSet.put(state.jobs, job)}}
    end
  end

  @impl true
  def handle_query({:get, _}, _from, state) do
    {:reply, {:ok, state}}
  end

  @impl true
  def handle_role_change(:follower, state) do
    for {job, ref} <- Process.get(:timers) do
      Logger.info("cancelling timer for #{inspect job}")

      :timer.cancel(ref)
    end

    state
  end
  def handle_role_change(_, state), do: state

  @impl true
  def handle_lease_taken(state) do
    for job <- state.jobs do
      start_timer(state.group_name, job)
    end

    state
  end

  @impl true
  def handle_info({:run, {{m, f, a}, _time} = job}, state) do
    spawn(m, f, a)

    start_timer(state.group_name, job)

    state
  end

  @impl true
  def receive_snapshot(snapshot, _state), do: snapshot

  @impl true
  def snapshot(state), do: state

  def dump(state), do: state

  defp start_timer(group_name, {mfa, time} = job) do
    if Craft.holding_lease?(group_name) do
      now = Time.utc_now()

      milliseconds_until_run =
        if Time.after?(time, now) do
          Time.diff(time, now, :millisecond)
        else
          Time.diff(time, now, :millisecond) + :timer.hours(24)
        end

      Logger.info("job #{inspect mfa} scheduled to run in #{:erlang.convert_time_unit(milliseconds_until_run, :millisecond, :second)} seconds")

      {:ok, ref} = :timer.apply_after(milliseconds_until_run, Craft, :send, [group_name, {:run, job}])

      timers = Process.get(:timers)
      Process.put(:timers, Map.put(timers, job, ref))
    end
  end
end
