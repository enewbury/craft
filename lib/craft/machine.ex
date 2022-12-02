defmodule Craft.Machine do
  use GenServer

  alias Craft.Log
  alias Craft.Log.Entry

  @type private :: any()

  @callback init(Craft.group_name()) :: {:ok, private()}
  @callback command(Craft.command(), Craft.log_index(), private()) :: {Craft.reply(), private()} | {Craft.reply(), Craft.side_effects(), private()}
  @callback last_applied_log_index(private()) :: Craft.log_index()

  defmodule State do
    defstruct [
      :module,
      :persistent,
      :private,
      last_applied: 0
    ]
  end

  def name(name), do: Module.concat(__MODULE__, name)

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: name(args.name))
  end

  # FIXME: document that for persistent machines, the commit index and the
  # state mutations need to be atomically commited to the persistent store
  #
  # otherwise a crash between the two operations would result in inconsistent
  # state when the machine picks up the log where it left off
  #
  # document that the consensus process sends the `log` state to this process,
  # and that craft assumes that the `log` is small (a handle), rather
  # than the whole log itself, the point of this is to allow the consensus process
  # to continue without being blocked by the machine process while it's applying
  # entries
  #
  def commit_index_bumped(group_name, new_commit_index, log) do
    group_name
    |> name()
    |> GenServer.cast({:commit_index_bumped, new_commit_index, log})
  end

  @impl true
  def init(args) do
    {:ok, private} = args.machine.init(args.name)

    state =
      %State{
        module: args.machine,
        persistent: args.machine.__craft_persistent__(),
        private: private
      }

    {:ok, state}
  end

  @impl true
  def handle_cast({:commit_index_bumped, new_commit_index, log, requests, is_leader?}, state) do
    last_applied_log_index =
      if state.persistent do
        state.module.last_applied(state.private)
      else
        state.last_applied
      end

    private =
      Enum.reduce(last_applied_log_index..new_commit_index, state.private, fn index, private ->
        %Entry{command: command} = Log.fetch(log, index)

        {reply, side_effects, private} =
          case state.module.command(command, index, private) do
            {reply, private} ->
              {reply, [], private}

            {reply, side_effects, private} ->
              {reply, side_effects, private}
          end

        if is_leader? do
          case Map.fetch(requests, index) do
            {:ok, {pid, _ref} = id} ->
              send(pid, {id, reply})

            _ ->
              :noop
          end

          Enum.each(side_effects, fn {m, f, a} ->
            spawn(fn -> apply(m, f, a) end)
          end)
        end

        private
      end)

    {:noreply, %State{state | private: private, last_applied: new_commit_index}}
  end


  def __using__(opts) do
    quote do
      # FIXME: better error
      persistent = Keyword.fetch!(unquote(opts), :persistent)
      def __craft_persistent__(), do: persistent
    end
  end
end
