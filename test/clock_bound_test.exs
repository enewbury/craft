defmodule Craft.ClockBoundTest do
  use ExUnit.Case, async: false

  alias Craft.GlobalTimestamp
  alias Craft.GlobalTimestamp.ClockBound

  test "happy path" do
    assert {:ok, %GlobalTimestamp{}} = ClockBound.now(fn -> {:ok, make_binary()} end)
  end

  test "segment size inconsistency" do
    assert {:error, :incorrect_segment_size} = ClockBound.now(fn -> {:ok, make_binary(segment_size: 100)} end)
  end

  test "unsupported version" do
    assert {:error, :incorrect_segment_version} = ClockBound.now(fn -> {:ok, make_binary(version: 3)} end)
  end

  describe "generation number" do
    test "when segment hasn't been initialized yet" do
      assert {:error, :segment_not_initialized} = ClockBound.now(fn -> {:ok, make_binary(generation: 0)} end)
    end

    test "when segment is undergoing an update" do
      assert {:error, :segment_undergoing_write} = ClockBound.now(fn -> {:ok, make_binary(generation: 1)} end)
    end
  end

  test "clock drift too large" do
    assert {:error, :max_drift_too_large} = ClockBound.now(fn -> {:ok, make_binary(max_drift: 1_000_000_001)} end)
  end

  test "clock not synchronized (free-running, disrupted, etc..)" do
    assert {:error, :clock_not_synchronized} = ClockBound.now(fn -> {:ok, make_binary(clock_status: 2)} end)
  end

  describe "validity window" do
    test "sample taken before `as_of`" do
      as_of_sec = :erlang.convert_time_unit(ClockBound.monotonic_time(), :nanosecond, :second) + 5

      assert {:error, :segment_expired_or_not_yet_valid} = ClockBound.now(fn -> {:ok, make_binary(as_of_sec: as_of_sec, as_of_nsec: 0)} end)
    end

    test "sample taken after `void_after`" do
      void_after_sec = :erlang.convert_time_unit(ClockBound.monotonic_time(), :nanosecond, :second) - 5

      assert {:error, :segment_expired_or_not_yet_valid} = ClockBound.now(fn -> {:ok, make_binary(void_after_sec: void_after_sec, void_after_nsec: 0)} end)
    end
  end

  test "retries" do
    Process.put(:should_retry, true)

    read_fun =
      fn ->
        if Process.get(:should_retry) do
          Process.put(:should_retry, false)
          send(self(), :retried)

          {:ok, make_binary(generation: 2)}
        else
          {:ok, make_binary()}
        end
      end

    assert {:ok, _} = ClockBound.now(read_fun, 1)

    assert_receive :retried
  end

  defp make_binary(args \\ []) do
    as_of = ClockBound.monotonic_time() - :erlang.convert_time_unit(5, :second, :nanosecond)
    as_of_sec = :erlang.convert_time_unit(as_of, :nanosecond, :second)
    as_of_nsec = as_of - as_of_sec * :erlang.convert_time_unit(1, :second, :nanosecond)

    void_after = ClockBound.monotonic_time() + :erlang.convert_time_unit(5, :second, :nanosecond)
    void_after_sec = :erlang.convert_time_unit(void_after, :nanosecond, :second)
    void_after_nsec = void_after - void_after_sec * :erlang.convert_time_unit(1, :second, :nanosecond)

    args =
      Map.merge(%{
        segment_size: 80,
        version: 2,
        generation: 2,
        as_of_sec: as_of_sec,
        as_of_nsec: as_of_nsec,
        void_after_sec: void_after_sec,
        void_after_nsec: void_after_nsec,
        bound: 500,
        disruption_marker: 0,
        max_drift: 1_000,
        clock_status: 1,
        clock_disruption_support: 0
      }, Map.new(args))

    <<0x414D5A4E :: unsigned-native-32,
      0x43420200 :: unsigned-native-32,
      args[:segment_size] :: unsigned-native-32,
      args[:version] :: unsigned-native-16,
      args[:generation] :: unsigned-native-16,
      args[:as_of_sec] :: signed-native-64,
      args[:as_of_nsec] :: signed-native-64,
      args[:void_after_sec] :: signed-native-64,
      args[:void_after_nsec] :: signed-native-64,
      args[:bound] :: signed-native-64,
      args[:disruption_marker] :: unsigned-native-64,
      args[:max_drift] :: unsigned-native-32,
      args[:clock_status] :: signed-native-32,
      args[:clock_disruption_support] :: unsigned-native-8,
      0 :: unit(8)-size(7) >>
  end
end
