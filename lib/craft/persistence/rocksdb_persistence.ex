defmodule Craft.Persistence.RocksDBPersistence do
  @moduledoc """
  Notes:
  - rocksdb's default comparator is lexicographic. when given positive integer terms, :erlang.term_to_binary/1
    outputs lexicographically ascending keys, so we can use rocks' default iterator to walk log indexes

  - remove `latest_index` and `latest_term` from struct and replace with non-cached iterator version
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
    :write_opts
  ]

  @impl true
  def new(group_name, opts \\ []) do
    data_dir = Keyword.get(opts, :data_dir, Path.join([File.cwd!(), "data", to_string(node()), group_name, "log"]))
    write_opts = Keyword.get(opts, :write_opts, [sync: true])

    File.mkdir_p!(data_dir)

    db_opts = [create_if_missing: true, create_missing_column_families: true]

    {:ok, db, [_default, log_column_family_handle, metadata_column_family_handle]} =
      data_dir
      |> :erlang.binary_to_list()
      |> :rocksdb.open_optimistic_transaction_db(db_opts, [{'default', []}, @log_column_family, @metadata_column_family])

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
      metadata_cf: metadata_column_family_handle,
      write_opts: write_opts
    }
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

    :ok = :rocksdb.write_batch(state.db, batch, state.write_opts)
    :ok = :rocksdb.release_batch(batch)

    %__MODULE__{state | latest_term: List.last(entries).term}
  end

  @impl true
  # unimplemented in optimistic mode, it seems :(
  # def rewind(%__MODULE__{latest_index: latest_index} = state, index) when index < latest_index do
    # :ok = :rocksdb.delete_range(state.db, state.log_cf, encode(index + 1), encode(state.latest_index + 1), state.write_opts)

  #   %__MODULE__{state | latest_index: index}
  # end
  # def rewind(state, _index), do: state

  def rewind(%__MODULE__{latest_index: latest_index} = state, index) when index < latest_index do
    {:ok, transaction} = :rocksdb.transaction(state.db, state.write_opts)
    {:ok, iterator} = :rocksdb.transaction_iterator(transaction, state.log_cf, [])

    do_rewind(transaction, iterator, state.log_cf, encode(index))

    :ok = :rocksdb.transaction_commit(transaction)

    %__MODULE__{state | latest_index: index}
  end
  def rewind(%__MODULE__{} = state, _index), do: state

  require Logger
  defp do_rewind(transaction, iterator, log_cf, min_index) do
    case :rocksdb.iterator_move(iterator, :last) do
      {:ok, index, _value} when index > min_index ->
        :ok = :rocksdb.transaction_delete(transaction, log_cf, index)
        do_rewind(transaction, iterator, log_cf, min_index)

      {:ok, _index, _value} ->
        :ok

      error ->
        raise error
    end
  end

  @impl true
  # the current version of rocksdb-erlang doesn't support delete_range in transactions, so we have to do it with an iterator
  # in order to maintain atomicity with the snapshot_entry insertion
  #
  # https://github.com/facebook/rocksdb/issues/4812
  def truncate(%__MODULE__{} = state, index, snapshot_entry) do
    {:ok, transaction} = :rocksdb.transaction(state.db, state.write_opts)
    {:ok, iterator} = :rocksdb.transaction_iterator(transaction, state.log_cf, [])

    do_truncate(transaction, iterator, state.log_cf, encode(index))

    :ok = :rocksdb.transaction_put(transaction, state.log_cf, encode(index), encode(snapshot_entry))
    :ok = :rocksdb.transaction_commit(transaction)

    state
  end

  defp do_truncate(transaction, iterator, log_cf, max_index) do
    case :rocksdb.iterator_move(iterator, :first) do
      {:ok, index, _value} when index <= max_index ->
        :ok = :rocksdb.transaction_delete(transaction, log_cf, index)
        do_truncate(transaction, iterator, log_cf, max_index)

      {:ok, _index, _value} ->
        :ok

      error ->
        raise error
    end
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
  def put_metadata(%__MODULE__{} = state, binary) do
    :ok = :rocksdb.put(state.db, state.metadata_cf, "metadata", binary, state.write_opts)

    state
  end

  @impl true
  def fetch_metadata(%__MODULE__{} = state) do
    case :rocksdb.get(state.db, state.metadata_cf, "metadata", []) do
      {:ok, binary} ->
        {:ok, binary}

      _ ->
        :error
    end
  end

  @impl true
  def dump(%__MODULE__{} = state) do
    Enum.flat_map([state.metadata_cf, state.log_cf], fn cf ->
      {:ok, iterator} = :rocksdb.iterator(state.db, cf, [])
      {:ok, index, value} = :rocksdb.iterator_move(iterator, :first)

      Stream.repeatedly(fn ->
        case :rocksdb.iterator_move(iterator, :next) do
          {:ok, index, value} ->
            {index, value}

          _ ->
            :ok = :rocksdb.iterator_close(iterator)
            :eof
        end
      end)
      |> Stream.take_while(fn
        :eof ->
          false

        _ ->
          true
      end)
      |> Enum.concat([{index, value}])
      |> Enum.map(fn {k, v} ->
        try do
          {decode(k), decode(v)}
        rescue
          _ ->
            {k, decode(v)}
        end
      end)
      |> Enum.sort()
    end)
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
