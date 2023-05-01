defmodule Craft.Persistence.RocksDBPersistence do
  @moduledoc """
  RocksDB Peristence Backend

  Notes:
  - rocksdb's default comparator is lexicographic. when given positive integer terms, :erlang.term_to_binary/1
    outputs lexicographically ascending keys, so we can use rocks' default iterator to walk log indexes

  """
  @behaviour Craft.Persistence

  @log_column_family {'log', []}
  @metadata_column_family {'metadata', []}

  defstruct [
    :db,
    :log_cf,
    :metadata_cf,
    :latest_index,
    :latest_term,
  ]

  @impl true
  def new(group_name, opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, Path.join(["data", File.cwd!(), to_string(node())]))

    File.mkdir_p!(data_dir)

    db_opts = [create_if_missing: true, create_missing_column_families: true]

    {:ok, db, [_default, log_column_family_handle, metadata_column_family_handle]} =
      data_dir
      |> Path.join(group_name)
      |> :erlang.binary_to_list()
      |> :rocksdb.open(db_opts, [{'default', []}, @log_column_family, @metadata_column_family])

    latest_index =
      with {:ok, iterator} <- :rocksdb.iterator(db, log_column_family_handle, []),
           {:ok, index, _value} <- :rocksdb.iterator_move(iterator, :last) do
        :ok = :rocksdb.iterator_close(iterator)
        decode(index)
      else
        {:error, :invalid_iterator} ->
          -1
      end

    %__MODULE__{
      db: db,
      latest_index: latest_index,
      log_cf: log_column_family_handle,
      metadata_cf: metadata_column_family_handle
    }
    |> put_current_term!(-1)
    |> put_voted_for!(nil)
  end

  @impl true
  def latest_index(%__MODULE__{} = state), do: state.latest_index

  @impl true
  def latest_term(%__MODULE__{} = state), do: state.latest_index

  @impl true
  def fetch(%__MODULE__{} = state, index) do
    case :rocksdb.get(state.db, state.log_cf, encode(index), []) do
      {:ok, entry} ->
        {:ok, decode(entry)}

      :not_found ->
        :error
    end
  end

  @impl true
  def fetch_from(%__MODULE__{} = state, index) do
    {:ok, iterator} = :rocksdb.iterator(state.db, state.log_cf, [])

    iterator
    |> do_fetch_from(index, [])
    |> Enum.map(&decode/1)
    |> Enum.reverse()
  end

  defp do_fetch_from(iterator, index, acc) do
    case :rocksdb.iterator_move(iterator, encode(index)) do
      {:ok, _index, value} ->
        do_fetch_from(iterator, index + 1, [value | acc])

      _ ->
        :ok = :rocksdb.iterator_close(iterator)
        acc
    end
  end

  @impl true
  def append(%__MODULE__{} = state, []), do: state
  def append(%__MODULE__{} = state, entries) do
    {:ok, batch} = :rocksdb.batch()

    state =
      Enum.reduce(entries, state, fn entry, state ->
        index = state.latest_index + 1
        :ok = :rocksdb.batch_put(batch, state.log_cf, encode(index), encode(entry))

        %__MODULE__{state | latest_index: index}
      end)

    :ok = :rocksdb.write_batch(state.db, batch, sync: true)
    :ok = :rocksdb.release_batch(batch)

    %__MODULE__{state | latest_term: List.last(entries).term}
  end

  @impl true
  def rewind(%__MODULE__{} = state, index) do
    :ok = :rocksdb.delete_range(state.db, state.log_cf, encode(index + 1), encode(state.latest_index + 1), sync: true)

    %__MODULE__{state | latest_index: index}
  end

  @impl true
  def reverse_find(%__MODULE__{} = state, fun) do
    {:ok, iterator} = :rocksdb.iterator(state.db, state.log_cf, [])
    do_reverse_find(iterator, state.latest_index, fun)
  end

  defp do_reverse_find(iterator, index, fun) do
    case :rocksdb.iterator_move(iterator, encode(index)) do
      {:ok, _index, value} ->
        entry = decode(value)

        if fun.(entry) do
          :ok = :rocksdb.iterator_close(iterator)
          entry
        else
          do_reverse_find(iterator, index - 1, fun)
        end

      _ ->
        :ok = :rocksdb.iterator_close(iterator)
        nil
    end
  end

  @impl true
  def put_current_term!(%__MODULE__{} = state, term) do
    :ok = :rocksdb.put(state.db, state.metadata_cf, "current_term", encode(term), sync: true)

    state
  end

  @impl true
  def put_voted_for!(%__MODULE__{} = state, voted_for) do
    :ok = :rocksdb.put(state.db, state.metadata_cf, "voted_for", encode(voted_for), sync: true)

    state
  end

  @impl true
  def get_current_term!(%__MODULE__{} = state) do
    {:ok, val} = :rocksdb.get(state.db, state.metadata_cf, "current_term", [])


    decode(val)
  end

  @impl true
  def get_voted_for!(%__MODULE__{} = state) do
    {:ok, val} = :rocksdb.get(state.db, state.metadata_cf, "voted_for", [])

    decode(val)
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)




  def dump(%__MODULE__{} = state) do
    Enum.each([state.metadata_cf, state.log_cf], fn cf ->
      {:ok, iterator} = :rocksdb.iterator(state.db, cf, [])
      {:ok, index, value} = :rocksdb.iterator_move(iterator, :first)
      print(index, value)
      dump(iterator)
      IO.puts("----------------------")
    end)
  end

  def dump(iterator) do
    case :rocksdb.iterator_move(iterator, :next) do
      {:ok, index, value} ->
        print(index, value)
        dump(iterator)

      _ ->
        :ok = :rocksdb.iterator_close(iterator)
    end
  end

  defp print(index, value) do
    try do
      IO.puts(inspect(decode(index)) <> " -> " <> inspect(decode(value)))
    rescue
      _ ->
      IO.puts(inspect(index) <> " -> " <> inspect(decode(value)))
    end
  end
end
