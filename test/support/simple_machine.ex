defmodule Craft.SimpleMachine do
  use Craft.Machine, persistent: false

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
