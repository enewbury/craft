defmodule Craft.Log do

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

  defstruct [
    :module,
    :state
  ]

  def new(group_name, log_module) do
    %__MODULE__{
      module: log_module,
      state: log_module.new(group_name)
    }
  end

  def latest_term(%__MODULE__{module: module, state: state}) do
    module.latest_term(state)
  end

  def latest_index(%__MODULE__{module: module, state: state}) do
    module.latest_index(state)
  end
end
