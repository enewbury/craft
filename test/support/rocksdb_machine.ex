defmodule Craft.RocksDBMachine do
  use Craft.Machine, persistent: true

  @log_index_column_family {~c"log_index", []}
  @log_index_key "log_index"

  def put(name, nodes, k, v) do
    Craft.command({:put, k, v}, name, nodes)
  end

  def get(name, nodes, k) do
    Craft.command({:get, k}, name, nodes)
  end

  defmodule State do
    defstruct [:db, :log_index_column_family, :snapshots_dir]
  end

  def init(group_name) do
    data_dir = Path.join([File.cwd!(), "data", to_string(node()), group_name, "machine"])

    db_opts = [create_if_missing: true, create_missing_column_families: true]

    {:ok, db, [_default, log_index_column_family]} =
      data_dir
      |> :erlang.binary_to_list()
      |> :rocksdb.open_optimistic_transaction_db(db_opts, [{~c"default", []}, @log_index_column_family])

    state =
      %State{
        db: db,
        log_index_column_family: log_index_column_family,
        snapshots_dir: Path.join(data_dir, "snapshots")
      }

    File.mkdir_p!(state.snapshots_dir)

    {:ok, state}
  end

  def command({:put, k, v}, log_index, state) do
    {:ok, batch} = :rocksdb.batch()
    :ok = :rocksdb.batch_put(batch, encode(k), encode(v))
    :ok = :rocksdb.batch_put(batch, state.log_index_column_family, @log_index_key, encode(log_index))
    :ok = :rocksdb.write_batch(state.db, batch, sync: true)
    :ok = :rocksdb.release_batch(batch)

    {:ok, state}
  end

  def command({:get, k}, _log_index, state) do
    case :rocksdb.get(state.db, encode(k), []) do
      {:ok, value} ->
        {{:ok, decode(value)}, state}

      :not_found ->
        {{:error, :not_found}, state}

      error ->
        {error, state}
    end
  end

  def snapshot(at_index, state) do
    path = Path.join(state.snapshots_dir, to_string(at_index))

    File.mkdir_p!(state.snapshots_dir)
    :ok = :rocksdb.checkpoint(state.db, :erlang.binary_to_list(path))

    path
  end

  def snapshots(state) do
    File.ls!(state.snapshots_dir)
  end

  def last_applied_log_index(state) do
    case :rocksdb.get(state.db, state.log_index_column_family, @log_index_key, []) do
      {:ok, value} ->
        decode(value)

      :not_found ->
        nil
    end
  end

  def dump(state) do
    kv =
      with {:ok, iterator} <- :rocksdb.iterator(state.db, []),
           {:ok, index, value} <- :rocksdb.iterator_move(iterator, :first) do
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
      else
        {:error, :invalid_iterator} ->
          %{}
      end

    %{kv: kv, last_applied_log_index: last_applied_log_index(state), snapshots: snapshots(state)}
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
