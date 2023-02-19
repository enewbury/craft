defmodule Craft.Log.MapLog do
  alias Craft.Log.EmptyEntry

  @behaviour Craft.Log

  @impl true
  def new(_group_name) do
    append(%{}, [%EmptyEntry{term: -1}])
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
  def rewind(map, index) when index < map_size(map) do
    map
    |> Map.delete(latest_index(map))
    |> rewind(index)
  end
  def rewind(map, _index), do: map
end
