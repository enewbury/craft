defmodule Craft.Log do
  alias Craft.Log.Entry

  # this module is kinda like a bastardized mix of a behaviour and a protocol
  #
  # it's a module in the sense that:
  # we want the user to hand us a module name as an option, but we don't want them
  # to instantiate it for us, we want to do that at init-time for the consensus process
  #
  # but it's a protocol in the sense that:
  # it'd just be nice to call (e.g.) Log.last_term(t()) and not have to carry the module
  # name around with us and wrap/unwrap it
  #
  # so yeah, if you have a better idea how to do this, holler at me please. :)
  #

  #TODO: proper typespecs
  @callback new(group_name :: String.t()) :: any()
  @callback latest_term(any()) :: integer()
  @callback latest_index(any()) :: integer()
  @callback fetch(any(), index :: pos_integer()) :: Entry.t()
  @callback append(any(), Entry.t()) :: any()

  defstruct [
    :module,
    :private
  ]

  def new(group_name, log_module) do
    %__MODULE__{
      module: log_module,
      private: log_module.new(group_name)
    }
    |> append(%Entry{term: 0})
  end

  def latest_term(%__MODULE__{module: module, private: private}) do
    module.latest_term(private)
  end

  def latest_index(%__MODULE__{module: module, private: private}) do
    module.latest_index(private)
  end

  def fetch(%__MODULE__{module: module, private: private}, index) do
    module.fetch(private, index)
  end

  def append(%__MODULE__{module: module, private: private} = log, entry) do
    %__MODULE__{log | private: module.append(private, entry)}
  end
end
