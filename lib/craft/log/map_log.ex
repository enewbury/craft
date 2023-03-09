defmodule Craft.Log.MapLog do
  @behaviour Craft.Log

  @impl true
  def new(_group_name) do
    %{}
  end

  @impl true
  def latest_term(map) do
    map
    |> Map.fetch!(latest_index(map))
    |> Map.fetch!(:term)
  end

  @impl true
  def latest_index(map) do
    map_size(map) - 1
  end

  @impl true
  defdelegate fetch(map, index), to: Map

  @impl true
  def fetch_from(map, index) do
    if index > latest_index(map) do
      []
    else
      Enum.map(index..latest_index(map), fn index ->
        {:ok, entry} = fetch(map, index)

        entry
      end)
    end
  end

  @impl true
  def append(map, entries) do
    Enum.reduce(entries, map, fn entry, map ->
      Map.put(map, map_size(map), entry)
    end)
  end

  @impl true
  def rewind(map, index) when index + 1 < map_size(map) do
    map
    |> Map.delete(latest_index(map))
    |> rewind(index)
  end
  def rewind(map, _index), do: map

  @impl true
  def reverse_find(map, fun) do
    Enum.find_value(latest_index(map)..0, fn i ->
      {:ok, entry} = fetch(map, i)

      if fun.(entry) do
        entry
      else
        false
      end
    end)
  end
end
