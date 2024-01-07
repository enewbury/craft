defmodule Craft.Persistence do
  alias Craft.Log.EmptyEntry
  alias Craft.Log.CommandEntry
  alias Craft.Log.MembershipEntry
  alias Craft.Log.SnapshotEntry

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

  @type entry :: EmptyEntry.t() | CommandEntry.t() | MembershipEntry.t() | SnapshotEntry.t()

  #TODO: proper typespecs
  @callback new(group_name :: String.t(), args :: any()) :: any()
  @callback latest_term(any()) :: integer()
  @callback latest_index(any()) :: integer()
  @callback fetch(any(), index :: integer()) :: entry()
  @callback fetch_from(any(), index :: integer()) :: [entry()]
  @callback append(any(), [entry()]) :: any()
  @callback rewind(any(), index :: integer()) :: any() # remove all long entries after index
  @callback truncate(any(), index :: integer(), SnapshotEntry.t()) :: any() # atomically remove log entries up to and including `index` and replace with SnapshotEntry
  @callback reverse_find(any(), fun()) :: entry() | nil
  @callback put_metadata(any(), binary()) :: any()
  @callback fetch_metadata(any()) :: {:ok, binary()} | :error
  @callback dump(any()) :: any()

  defstruct [
    :module,
    :private
  ]

  defmodule Metadata do
    alias Craft.Consensus.State
    alias Craft.Persistence

    defstruct [
      :current_term,
      :voted_for
    ]

    def init(%State{} = state) do
      write(%__MODULE__{}, state)
    end

    def fetch(%Persistence{module: module, private: private}) do
      case module.fetch_metadata(private) do
        {:ok, binary} ->
          {:ok, struct(__MODULE__, :erlang.binary_to_term(binary))}

        :error ->
          :error
      end
    end

    def update(%State{} = state) do
      get_and_update(state, fn metadata ->
        %__MODULE__{
          metadata |
          current_term: state.current_term,
          voted_for: state.voted_for
        }
      end)
    end

    defp get_and_update(%State{persistence: persistence} = state, fun) do
      metadata =
        case fetch(persistence) do
          {:ok, metadata} ->
            metadata

          :error ->
            raise "couldn't fetch metadata"
        end

      metadata
      |> fun.()
      |> write(state)
    end

    defp write(metadata, %State{persistence: %Persistence{module: module, private: private}} = state) do
      dumped =
        metadata
        |> Map.from_struct()
        |> :erlang.term_to_binary()

      persistence = %Persistence{state.persistence | private: module.put_metadata(private, dumped)}

      %State{state | persistence: persistence}
    end
  end

  #
  # Craft initializes the log with a starter entry, like so:
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

  def truncate(%__MODULE__{module: module, private: private} = persistence, index, %SnapshotEntry{} = snapshot_entry) do
    %__MODULE__{persistence | private: module.truncate(private, index, snapshot_entry)}
  end

  def reverse_find(%__MODULE__{module: module, private: private} = persistence, fun) do
    %__MODULE__{persistence | private: module.reverse_search(private, fun)}
  end

  def dump(%__MODULE__{module: module, private: private}) do
    module.dump(private)
  end
end
