defmodule Craft.GlobalTimestamp do
  @moduledoc """
  Behaviour specifying a global clock with bounded error.
  """

  alias Craft.Consensus.State

  defstruct [:earliest, :latest]

  @type t :: %__MODULE__{
    earliest: DateTime.t(),
    latest: DateTime.t()
  }

  @callback now() :: {:ok, t()} | {:error, :try_again} | :error

  def now(%State{global_clock: global_clock}) when is_atom(global_clock), do: global_clock.now()
  def now(_), do: {:ok, nil}
end
