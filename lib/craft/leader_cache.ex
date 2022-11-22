defmodule Craft.LeaderCache do
  def init do
    :ets.new(__MODULE__, [:set, :named_table, :public, read_concurrency: true])
  end

  def put(group_name, node) do
    :ets.insert(__MODULE__, {group_name, node})
  end

  def get(group_name) do
    case :ets.lookup(__MODULE__, group_name) do
      [{^group_name, leader}] ->
        {:ok, leader}

      [] ->
        :not_found
    end
  end

  #def delete(group_name)
end
