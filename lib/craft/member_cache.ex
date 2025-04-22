defmodule Craft.MemberCache do
  use GenServer

  alias Craft.Consensus.State
  alias Craft.Consensus.State.Members

  def discover(group_name, initial_nodes) do
    :ets.insert(__MODULE__, {group_name, nil, MapSet.new(initial_nodes)})

    send(__MODULE__, :start_polling)

    :ok
  end

  def all do
    :ets.foldr(& [&1 | &2], [], __MODULE__)
    |> Map.new(fn {group_name, leader, members} ->
      {group_name, {leader, members}}
    end)
  end

  @doc false
  def update(%State{} = state) do
    update(state.name, state.leader_id, Members.all_nodes(state.members))
  end

  defp update(group_name, leader, members) do
    :ets.insert(__MODULE__, {group_name, leader, members})
  end

  @doc false
  def update_leader(group_name, new_leader) do
    :ets.update_element(__MODULE__, group_name, {2, new_leader})
  end

  @doc false
  def get(group_name) do
    case :ets.lookup(__MODULE__, group_name) do
      [{^group_name, leader, members}] ->
        {:ok, leader, members}

      [] ->
        :not_found
    end
  end

  @doc false
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl GenServer
  def init(_args) do
    :ets.new(__MODULE__, [:set, :named_table, :public, read_concurrency: true])

    {:ok, false}
  end

  @impl GenServer
  def handle_call({:get, group_name}, _from, state) do
    {:reply, get(group_name), state}
  end

  @impl GenServer
  def handle_info(:start_polling, false) do
    send(self(), :poll)

    {:noreply, true}
  end
  def handle_info(:start_polling, state), do: {:noreply, state}

  @impl GenServer
  def handle_info(:poll, true) do
    for {group_name, {leader, members}} <- all() do
      do_poll(group_name, [leader | Enum.to_list(members)])
    end

    Process.send_after(self(), :poll, 5_000)

    {:noreply, true}
  end

  defp do_poll(_group_name, []), do: :not_found
  defp do_poll(group_name, [node | nodes]) do
    try do
      case GenServer.call({__MODULE__, node}, {:get, group_name}) do
        {:ok, leader, members} ->
          update(group_name, leader, members)
          :ok

        :not_found ->
          do_poll(group_name, nodes)
      end
    catch :exit, _e ->
      do_poll(group_name, nodes)
    end
  end
end
