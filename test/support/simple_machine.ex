defmodule Craft.SimpleMachine do
  use Craft.Machine, mutable: false
  alias Craft.Linearizability.TestModel

  @behaviour TestModel

  def put(name, k, v) do
    Craft.command({:put, k, v}, name)
  end

  def get(name, k) do
    Craft.query({:get, k}, name, consistency: :linearizable)
  end

  @impl TestModel
  def init, do: init(nil)

  def init(_group_name) do
    {:ok, %{}}
  end

  @impl TestModel
  def command(command, state), do: command(command, nil, state)

  def command({:put, k, v}, _log_index, state) do
    {:ok, Map.put(state, k, v)}
  end

  @impl TestModel
  def query({:get, k}, state) do
    {:ok, Map.get(state, k)}
  end

  @impl true
  def receive_snapshot(snapshot, _state) do
    snapshot
  end

  @impl true
  def snapshot(state) do
    state
  end

  def dump(state), do: state
end
