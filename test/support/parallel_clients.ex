defmodule Craft.ParallelClients do
  @moduledoc false

  alias Craft.Linearizability.Operation

  def run(fun, num, num_requests) do
    1..num
    |> Task.async_stream(fn _ ->
      for i <- 1..num_requests do
        do_request(fun, i)
      end
    end, timeout: :infinity)
    |> Enum.flat_map(fn {:ok, ops} -> ops end)
  end

  def start(fun, num) do
    for _ <- 1..num do
      spawn_link(fn -> loop(fun) end)
    end
  end

  defp loop(fun, {i, ops} \\ {0, []}) do
    receive do
      {:stop, pid} ->
        send(pid, {self(), ops})
    after
      0 ->
        loop(fun, {i+1, [do_request(fun, i) | ops]})
    end
  end

  def stop(pids) do
    for pid <- pids do
      send(pid, {:stop, self()})
    end

    for pid <- pids do
      receive do
        {^pid, ops} ->
          ops
      end
    end
    |> List.flatten()
  end

  defp do_request(fun, i) do
    called_at = :erlang.monotonic_time()
    {request, response} = fun.(i)
    received_at = :erlang.monotonic_time()

    %Operation{
      id: {self(), called_at},
      client: self(),
      called_at: called_at,
      received_at: received_at,
      request: request,
      response: response
    }
  end
end
