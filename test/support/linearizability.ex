defmodule Craft.Linearizability do

  @history [
   %{
     client: 1,
     called_at: 3,
     received_at: 4,
     command: {:put, :a, 1},
     response: :ok
   },
   %{
     client: 2,
     called_at: 4,
     received_at: 5,
     command: {:get, :a},
     response: {:ok, 1}
   },

   %{
     client: 3,
     called_at: 4,
     received_at: 5,
     command: {:get, :a},
     response: {:ok, 1}
   },

   %{
     client: 4,
     called_at: 5,
     received_at: 6,
     command: {:get, :a},
     response: {:ok, 2}
   },
  ]

  def run_static do
    linearizable?(@history, Craft.SimpleMachine)
  end

  def run do
    File.read!("history")
    |> :erlang.binary_to_term()
    |> linearizable?(Craft.SimpleMachine)
  end

  # wing & gong & lowe algo
  def linearizable?([], _model), do: true
  def linearizable?(history, model) do
    {:ok, model_state} = model.init("abc")

    result =
      history
      |> Enum.group_by(& &1.client)
      |> Map.new(fn {client, ops} ->
        ops = Enum.sort_by(ops, & &1.called_at)
        {client, ops}
      end)
      |> do_linearizable({model, model_state})

    case result do
      true ->
        true

      _ ->
        false
    end
  end

  defp do_linearizable(histories_by_client, {model, model_state}, ops_seen \\ MapSet.new(), cache \\ MapSet.new()) do
    no_ops_left? = Enum.all?(histories_by_client, fn {_client, history} -> Enum.empty?(history) end)

    if no_ops_left? do
      true
    else
      first_ops =
        Enum.flat_map(histories_by_client, fn
          {_client, []} -> []
          {_client, [op | _]} -> [op]
        end)

      first_return = Enum.min_by(first_ops, & &1.received_at).received_at

      minimal_ops = Enum.reject(first_ops, & &1.called_at > first_return)

      Enum.reduce_while(minimal_ops, cache, fn op, cache ->
        {model_response, new_model_state} = model.command(op.command, nil, model_state)

        if model_response == op.response do
          history_without_op = tl(histories_by_client[op.client])
          histories_by_client_without_op = Map.put(histories_by_client, op.client, history_without_op)
          ops_seen = MapSet.put(ops_seen, op)
          cache_key = {ops_seen, new_model_state}

          if MapSet.member?(cache, cache_key) do
            {:cont, cache}
          else
            case do_linearizable(histories_by_client_without_op, {model, new_model_state}, ops_seen, cache) do
              true ->
                {:halt, true}

              cache ->
                {:cont, MapSet.put(cache, cache_key)}
            end
          end
        else
          {:cont, cache}
        end
      end)
    end
  end
end
