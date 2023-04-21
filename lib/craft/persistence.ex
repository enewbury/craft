defmodule Craft.Persistence do
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
  # it'd just be nice to call (e.g.) Persistence.latest_term(t()) and not have to carry the module
  # name around with us and wrap/unwrap it
  #
  # so yeah, if you have a better idea how to do this, holler at me please. :)
  #

  @type entry :: EmptyEntry.t() | CommandEntry.t() | MembershipEntry.t()

  #TODO: proper typespecs
  @callback new(group_name :: String.t(), args :: any()) :: any()
  @callback latest_term(any()) :: integer()
  @callback latest_index(any()) :: integer()
  @callback fetch(any(), index :: integer()) :: entry()
  @callback fetch_from(any(), index :: integer()) :: [entry()]
  @callback append(any(), [entry()]) :: any()
  @callback rewind(any(), index :: integer()) :: any() # remove all long entries after index
  @callback reverse_find(any(), fun()) :: entry() | nil
  @callback put_current_term!(any(), integer()) :: any()
  @callback put_voted_for!(any(), integer()) :: any()

  defstruct [
    :module,
    :private
  ]

  #
  # Craft requires that log implementations are initialized at following point:
  #
  # log: 0 -> EmptyEntry{term: -1}
  #
  # this makes the rest of the codebase a lot simpler
  #
  def new(group_name, {module, args}) do
    %__MODULE__{
      module: module,
      private: module.new(group_name, args)
    }
    |> append(%EmptyEntry{term: -1})
  end

  # Consensus Metadata

  def voted_for(%__MODULE__{module: module, private: private}, voted_for) do
    module.voted_for(private, voted_for)
  end

  def voted_for(%__MODULE__{module: module, private: private}) do
    module.voted_for(private)
  end

  def current_term(%__MODULE__{module: module, private: private}, current_term) do
    module.current_term(private, current_term)
  end

  def current_term(%__MODULE__{module: module, private: private}) do
    module.current_term(private)
  end

  # Log

  def latest_term(%__MODULE__{module: module, private: private}) do
    module.latest_term(private)
  end

  def latest_index(%__MODULE__{module: module, private: private}) do
    module.latest_index(private)
  end

  def fetch(%__MODULE__{module: module, private: private}, index) do
    module.fetch(private, index)
  end

  def fetch_from(%__MODULE__{module: module, private: private}, index) do
    module.fetch_from(private, index)
  end

  # FIXME: rename to append!
  def append(%__MODULE__{module: module, private: private} = persistence, entries) when is_list(entries) do
    %__MODULE__{persistence | private: module.append(private, entries)}
  end
  def append(persistence, entry), do: append(persistence, [entry])

  def rewind(%__MODULE__{module: module, private: private} = persistence, index) do
    %__MODULE__{persistence | private: module.rewind(private, index)}
  end

  def reverse_find(%__MODULE__{module: module, private: private} = persistence, fun) do
    %__MODULE__{persistence | private: module.reverse_search(private, fun)}
  end

  def put_current_term!(%__MODULE__{module: module, private: private} = persistence, term) do
    %__MODULE__{persistence | private: module.put_current_term!(private, term)}
  end

  def put_voted_for!(%__MODULE__{module: module, private: private} = persistence, term) do
    %__MODULE__{persistence | private: module.put_voted_for!(private, term)}
  end

  def get_current_term!(%__MODULE__{module: module, private: private}) do
    module.get_current_term!(private)
  end

  def get_voted_for!(%__MODULE__{module: module, private: private}) do
    module.get_voted_for!(private)
  end
end
