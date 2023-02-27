defmodule Craft.Log do
  alias Craft.Log.EmptyEntry
  alias Craft.Log.CommandEntry
  alias Craft.Log.MembershipEntry

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

  @type entry :: EmptyEntry.t() | CommandEntry.t() | MembershipEntry.t()

  #TODO: proper typespecs
  @callback new(group_name :: String.t()) :: any()
  @callback latest_term(any()) :: integer()
  @callback latest_index(any()) :: integer()
  @callback fetch(any(), index :: integer()) :: entry()
  @callback fetch_from(any(), index :: integer()) :: [entry()]
  @callback append(any(), [entry()]) :: any()
  @callback rewind(any(), index :: integer()) :: any() # remove all long entries after index

  defstruct [
    :module,
    :private
  ]

  #
  # Craft requires that log implementations are initialized at following point:
  #
  # last_applied: 0
  # log: 0 -> CommandEntry{term: -1, command: nil}
  #
  # this makes the rest of the codebase a lot simpler
  #
  def new(group_name, log_module) do
    %__MODULE__{
      module: log_module,
      private: log_module.new(group_name)
    }
    |> append(%EmptyEntry{term: -1})
  end

  def latest_term(%__MODULE__{module: module, private: private}) do
    module.latest_term(private)
  end

  def latest_index(%__MODULE__{module: module, private: private}) do
    module.latest_index(private)
  end

  def last_applied(%__MODULE__{module: module, private: private}) do
    module.last_applied(private)
  end

  def increment_last_applied(%__MODULE__{module: module, private: private}) do
    module.increment_last_applied(private)
  end

  def fetch(%__MODULE__{module: module, private: private}, index) do
    module.fetch(private, index)
  end

  def fetch_from(%__MODULE__{module: module, private: private}, index) do
    module.fetch_from(private, index)
  end

  def append(%__MODULE__{module: module, private: private} = log, entries) when is_list(entries) do
    %__MODULE__{log | private: module.append(private, entries)}
  end
  def append(log, entry), do: append(log, [entry])

  def rewind(%__MODULE__{module: module, private: private} = log, index) do
    %__MODULE__{log | private: module.rewind(private, index)}
  end
end
