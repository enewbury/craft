defmodule Craft.ParallelClients do
  @moduledoc false

  alias Craft.Linearizability.Operation
  alias Craft.GlobalTimestamp.NativeClock

  import Craft.Tracing, only: [logger_metadata: 1]

  require Logger

  def run(fun, num, num_requests, nexus) do
    1..num
    |> Task.async_stream(fn _ ->
      for i <- 1..num_requests do
        do_request(fun, i, nexus)
      end
    end, timeout: :infinity)
    |> Enum.flat_map(fn {:ok, ops} -> ops end)
  end

  def start(fun, num, nexus) do
    for _ <- 1..num do
      spawn_link(fn -> loop(fun, nexus) end)
    end
  end

  defp loop(fun, nexus, {i, ops} \\ {0, []}) do
    receive do
      {:stop, pid} ->
        send(pid, {self(), ops})
    after
      0 ->
        loop(fun, nexus, {i+1, [do_request(fun, i, nexus) | ops]})
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

  defp do_request(fun, i, nexus) do
    called_at = NativeClock.monotonic_raw_time()
    {request, response} = fun.(i)
    received_at = NativeClock.monotonic_raw_time()

    operation =
      %Operation{
        id: {self(), called_at},
        client: self(),
        called_at: called_at,
        received_at: received_at,
        request: request,
        response: response
      }

    Logger.info("request sent", logger_metadata(trace: {:client_request, :sent, operation}, t: called_at, node: node(), nexus: nexus, actor: self()))
    Logger.info("request received", logger_metadata(trace: {:client_request, :received, operation}, t: received_at, node: node(), nexus: nexus, actor: self()))

    operation
  end
end
