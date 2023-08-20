defmodule Craft.RocksDBMachine do
  use Craft.Machine, persistent: true

  @log_index_column_family {'log_index', []}
  @log_index_key "log_index"

  def put(name, nodes, k, v) do
    Craft.command({:put, k, v}, name, nodes)
  end

  def get(name, nodes, k) do
    Craft.command({:get, k}, name, nodes)
  end


  def init(group_name) do
    data_dir = Path.join([File.cwd!(), "data", to_string(node()), group_name, "machine"])
    snapshots_dir = Path.join(data_dir, "snapshots")

    File.mkdir_p!(snapshots_dir)

    db_opts = [create_if_missing: true, create_missing_column_families: true]

    {:ok, db, [_default, log_index_column_family]} =
      data_dir
      |> :erlang.binary_to_list()
      |> :rocksdb.open_optimistic_transaction_db(db_opts, [{'default', []}, @log_index_column_family])

    {:ok, {db, log_index_column_family}}
  end

  def command({:put, k, v}, log_index, {db, log_index_column_family} = state) do
    {:ok, batch} = :rocksdb.batch()
    :ok = :rocksdb.batch_put(batch, encode(k), encode(v))
    :ok = :rocksdb.batch_put(batch, log_index_column_family, @log_index_key, encode(log_index))
    :ok = :rocksdb.write_batch(db, batch, sync: true)
    :ok = :rocksdb.release_batch(batch)

    {:ok, state}
  end

  def command({:get, k}, _log_index, {db, _} = state) do
    case :rocksdb.get(db, encode(k), []) do
      {:ok, value} ->
        {{:ok, decode(value)}, state}

      :not_found ->
        {{:error, :not_found}, state}

      error ->
        {error, state}
    end
  end

  def snapshot(at_index, state) do
    IO.inspect state, label: "SNAPSHOTTING #{inspect at_index}"
  end

  def last_applied_log_index({db, log_index_column_family}) do
    case :rocksdb.get(db, log_index_column_family, @log_index_key, []) do
      {:ok, value} ->
        decode(value)

      :not_found ->
        nil
    end
  end

  def dump({db, _} = state) do
    {:ok, iterator} = :rocksdb.iterator(db, [])
    {:ok, index, value} = :rocksdb.iterator_move(iterator, :first)

    kv =
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

    %{kv: kv, last_applied_log_index: last_applied_log_index(state)}
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
