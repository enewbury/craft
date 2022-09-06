defmodule Craft.Log.MapLog do
  @behaviour Craft.Log

  defstruct [
    {:map, %{}}
  ]

  defmodule Entry do
    defstruct [
      :term,
      :command
    ]
  end

  @impl true
  def new(_group_name) do
    %__MODULE__{}
  end

  @impl true
  def last_term(%__MODULE__{map: map}) when map_size(map) > 0 do
    %Entry{term: term} = Map.fetch!(map, map_size(map))

    term
  end
  def last_term(%__MODULE__{}), do: -1

  @impl true
  def last_index(%__MODULE__{map: map}) do
    map_size(map)
  end
end
