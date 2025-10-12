defmodule Craft.GlobalTimestamp.NativeClock do
  @moduledoc "NIF interface to native clock functions"

  @on_load :load_nif

  def load_nif do
    Application.app_dir(:craft, ["priv", "clock_monotonic"]) |> :erlang.load_nif(0)
  end
  def clock_monotonic, do: :erlang.nif_error(:nif_not_loaded)
  def clock_monotonic_coarse, do: :erlang.nif_error(:nif_not_loaded)
  def clock_monotonic_raw, do: :erlang.nif_error(:nif_not_loaded)

  def monotonic_raw_time do
    clock_monotonic_raw() |> timespec_to_nanosecond()
  end

  def timespec_to_nanosecond({sec, nsec}) do
    :erlang.convert_time_unit(sec, :second, :nanosecond) + nsec
  end
end
