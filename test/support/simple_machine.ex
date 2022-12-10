defmodule Craft.SimpleMachine do
  use Craft.Machine, persistent: false

  def put(name, nodes, k, v) do
    Craft.command({:put, k, v}, name, nodes)
  end

  def get(name, nodes, k) do
    Craft.command({:get, k}, name, nodes)
  end

  def init(_group_name) do
    {:ok, %{}}
  end

  def command({:put, k, v}, _log_index, state) do
    {:ok, Map.put(state, k, v)}
  end

  def command({:get, k}, _log_index, state) do
    {{:ok, Map.get(state, k)}, state}
  end
end
