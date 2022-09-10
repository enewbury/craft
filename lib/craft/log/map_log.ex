defmodule Craft.Log.MapLog do
  @behaviour Craft.Log

  defmodule Entry do
    defstruct [
      :term,
      :command
    ]
  end

  @impl true
  def new(_group_name) do
    %{}
  end

  @impl true
  def latest_term(map) when map_size(map) > 0 do
    %Entry{term: term} = Map.fetch!(map, map_size(map))

    term
  end
  def latest_term(_), do: -1

  @impl true
  def latest_index(map) do
    map_size(map)
  end
end
