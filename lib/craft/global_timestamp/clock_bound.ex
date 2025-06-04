defmodule Craft.GlobalTimestamp.ClockBound do
  @moduledoc """
    Client for AWS' ClockBound daemon, which provides global clock error bounds via `chronyd`.

    https://github.com/aws/clock-bound
    https://github.com/aws/clock-bound/blob/main/docs/PROTOCOL.md
  """

  alias Craft.GlobalTimestamp

  @on_load :load_nif

  def now(path \\ Application.get_env(:craft, :clock_bound_shm_path), retries \\ 1_000)

  def now(path, retries) do
    with {:ok, binary} <- File.read(path),
        <<0x414D5A4E :: unsigned-native-32,
          0x43420200 :: unsigned-native-32,
          segment_size :: unsigned-native-32,
          version :: unsigned-native-16,
          generation :: unsigned-native-16,
          as_of_sec :: signed-native-64,
          as_of_nsec :: signed-native-64,
          void_after_sec :: signed-native-64,
          void_after_nsec :: signed-native-64,
          bound :: signed-native-64,
          _disruption_marker :: unsigned-native-64,
          max_drift :: unsigned-native-32,
          clock_status :: signed-native-32,
          _clock_disruption_support :: unsigned-native-8,
          _padding :: binary >> <- binary do

      as_of = timespec_to_nanosecond({as_of_sec, as_of_nsec})
      void_after = timespec_to_nanosecond({void_after_sec, void_after_nsec})

      # sandwich the wall-clock call between two montotonic clock calls, then later verify
      # that the they're within the segment's validity window (as_of and void_after)
      monotonic_before = monotonic_time()
      real = :os.system_time(:nanosecond)
      monotonic_after = monotonic_time()

      cond do
        segment_size != byte_size(binary) ->
          {:error, :incorrect_segment_size}

        generation == 0 ->
          if retries == 0 do
            {:error, :segment_not_initialized}
          else
            now(path, retries - 1)
          end

        rem(generation, 2) != 0 ->
          if retries == 0 do
            {:error, :segment_undergoing_write}
          else
            now(path, retries - 1)
          end

        version != 2 ->
          {:error, :incorrect_segment_version}

        max_drift > 1_000_000_000 ->
          {:error, :max_drift_too_large}

        clock_status != 1 ->
          {:error, :clock_not_synchronized}

        # the segment is expired (or not yet valid), let's try one more time before bailing out
        monotonic_before < as_of or monotonic_after > void_after ->
          if retries == 0 do
            {:error, :segment_expired_or_not_yet_valid}
          else
            now(path, retries - 1)
          end

        true ->
          # Increase the error bound with the maximum drift the clock may have experienced between
          # the time the clockbound data was written and now.
          #
          # max_drift is the number of nanoseconds the clock has drifted per second elapsed
          duration_sec = :erlang.convert_time_unit(monotonic_before - as_of, :nanosecond, :second)
          bound = bound + duration_sec * max_drift

          earliest = DateTime.from_unix!(real - bound, :nanosecond)
          latest = DateTime.from_unix!(real + bound, :nanosecond)

          {:ok, %GlobalTimestamp{earliest: earliest, latest: latest}}
      end
    else
      _error ->
        :error
    end
  end

  def load_nif do
    Application.app_dir(:craft, ["priv", "clock_monotonic"]) |> :erlang.load_nif(0)
  end
  def clock_monotonic, do: :erlang.nif_error(:nif_not_loaded)
  def clock_monotonic_coarse, do: :erlang.nif_error(:nif_not_loaded)

  defp monotonic_time do
    timespec =
      case clock_monotonic_coarse() do
        :error ->
          clock_monotonic()

        timespec ->
          timespec
      end

    timespec_to_nanosecond(timespec)
  end

  defp timespec_to_nanosecond({sec, nsec}) do
    :erlang.convert_time_unit(sec, :second, :nanosecond) + nsec
  end
end
