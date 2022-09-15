defmodule Craft.Log.MapLog do
  alias Craft.Log.Entry

  @behaviour Craft.Log

  @impl true
  def new(_group_name) do
    %{}
  end

  @impl true
  def latest_term(map) do
    %Entry{term: term} = Map.fetch!(map, latest_index(map))

    term
  end

  @impl true
  def latest_index(map) do
    map_size(map) - 1
  end

  @impl true
  defdelegate fetch(map, index), to: Map

  @impl true
  def append(map, entries) do
    Enum.reduce(entries, map, fn entry, map ->
      Map.put(map, map_size(map), entry)
    end)
  end

  # 0 1 2 3 4 5  size: 6
  # rewind to 3

  @impl true
  def rewind(map, index) when index < map_size(map) - 1 do
    map
    |> Map.delete(latest_index(map))
    |> rewind(index)
  end
  def rewind(map, _index), do: map
end
