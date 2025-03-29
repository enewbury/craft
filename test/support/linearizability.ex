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
     called_at: 1,
     received_at: 2,
     command: {:get, :a},
     response: {:ok, 1}
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

  # wing & gong algo
  def linearizable?([], _model), do: true
  def linearizable?(history, model) do
    {:ok, model_state} = model.init("abc")

    try do
      history
      |> Enum.group_by(fn %{client: client} -> client end)
      |> Map.new(fn {client, ops} ->
        ops = Enum.sort_by(ops, fn %{called_at: called_at} -> called_at end)
        {client, ops}
      end)
      |> do_linearizable?({model, model_state})
    catch
      {:linearizable, true} ->
        true
    else
      false ->
        false
    end
  end

  defp do_linearizable?(histories_by_client, {model, model_state}) do
    no_ops_left? =
      histories_by_client
      |> Map.values()
      |> Enum.all?(&Enum.empty?/1)

    if no_ops_left? do
      throw {:linearizable, true}
    end

    first_ops =
      Enum.flat_map(histories_by_client, fn
        {_client, []} -> []
        {_client, [op | _]} -> [op]
      end)

    first_return = Enum.min_by(first_ops, & &1.received_at).received_at

    minimal_ops = Enum.reject(first_ops, & &1.called_at > first_return)

    for op <- minimal_ops do
      {model_response, new_model_state} = model.command(op.command, nil, model_state)

      if model_response == op.response do
        history_without_op = tl(histories_by_client[op.client])
        histories_by_client_without_op = Map.put(histories_by_client, op.client, history_without_op)

        do_linearizable?(histories_by_client_without_op, {model, new_model_state})
      end
    end

    false
  end
end
