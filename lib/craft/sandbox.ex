defmodule Craft.Sandbox do
  use GenServer

  alias Craft.Configuration

  import Craft.Application, only: [via: 2, lookup: 2]

  @doc false
  def start_group(name, nodes, machine, opts) do
    GenServer.call(find!(), {:start_group, name, nodes, machine, opts})
  end

  @doc false
  def stop_group(name) do
    GenServer.call(find!(), {:stop_member, name})
  end

  @doc false
  def start_member(name, opts) do
    GenServer.call(find!(), {:start_member, name, opts})
  end

  @doc false
  def stop_member(name) do
    GenServer.call(find!(), {:stop_member, name})
  end

  @doc false
  def command(command, name, _opts) do
    GenServer.call(find!(), {:command, name, command})
  end

  @doc false
  def query(query, name, _opts) do
    GenServer.call(find!(), {:query, name, query})
  end

  @doc false
  def send(name, message) do
    GenServer.call(find!(), {:user_message, name, message})
  end

  @doc false
  def now do
    Craft.GlobalTimestamp.FixedError.now()
  end

  @doc false
  def backup(name, path) do
    GenServer.call(find!(), {:backup, name, path})
  end

  @doc false
  def restore(path) do
    GenServer.call(find!(), {:restore, path})
  end

  @doc false
  def holding_lease?(_name), do: true
  @doc false
  def holding_lease?(), do: true

  @doc false
  def known_groups(), do: :not_implemented
  @doc false
  def add_member(_name, _node, _opts), do: :not_implemented
  @doc false
  def remove_member(_name, _node, _opts), do: :not_implemented
  @doc false
  def transfer_leadership(_name), do: :not_implemented
  @doc false
  def transfer_leadership(_name, _to_node), do: :not_implemented
  @doc false
  def discover(_name, _nodes), do: :not_implemented
  @doc false
  def step_down(_name), do: :not_implemented
  @doc false
  def cached_info(_name), do: :not_implemented
  @doc false
  def purge(_name), do: :not_implemented


  @doc "Enters the specified process into the sandbox given by `name`"
  def join(name, pid \\ self()) do
    GenServer.call(__MODULE__.Manager, {:join, name, pid})
  end

  @doc false
  def find!(pid \\ self()) do
    with {:ok, name} <- GenServer.call(__MODULE__.Manager, {:find, pid}),
         pid when is_pid(pid) <- lookup(name, Craft.Sandbox) do
       pid

    else
      _ ->
        raise "no sandbox configured for pid #{inspect self()}, you must join a sandbox with Craft.Sandbox.join/1"
    end
  end

  @doc false
  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: via(name, __MODULE__))
  end

  defmodule MachineState do
    defstruct [
      :module,
      :private,
      index: 0
    ]
  end

  @impl GenServer
  def init(_) do
    {:ok, %{}}
  end

  @impl GenServer
  def handle_call({:start_group, name, nodes, machine, opts}, _from, state) do
    if state[name] do
      {:reply, {:error, :already_started}, state}
    else
      if !Configuration.find(name, namespace()) do
        config = %{
          machine: machine,
          nodes: nodes,
          global_clock: opts[:global_clock]
        }

        Configuration.write_new!(name, config, namespace())
      end

      {:reply, :ok, init_machine(name, machine, state)}
    end
  end

  def handle_call({:start_member, name, _opts}, _from, state) do
    if state[name] do
      {:reply, {:error, :already_started}, state}
    else
      if config = Configuration.find(name, namespace()) do
        {:reply, :ok, init_machine(name, config.machine, state)}
      else
        {:reply, {:error, :unknown_group}, state}
      end
    end
  end

  def handle_call({:stop_member, name, _opts}, _from, state) do
    if state[name] do
      {:reply, :ok, Map.delete(state, name)}
    else
      {:reply, {:error, :unknown_group}, state}
    end
  end

  def handle_call({:command, name, command}, _from, state) do
    if machine_state = state[name] do
      {reply, private} = machine_state.module.handle_command(command, machine_state.index, machine_state.private)

      {:reply, reply, Map.put(state, name, %{machine_state | private: private, index: machine_state.index+1})}
    else
      {:reply, {:error, :unknown_group}, state}
    end
  end

  def handle_call({:query, name, query}, from, state) do
    if machine_state = state[name] do
      case machine_state.module.handle_query(query, {:direct, from}, machine_state.private) do
        {:reply, response} ->
          {:reply, response, state}

        :noreply ->
          {:noreply, state}
      end
    else
      {:reply, {:error, :unknown_group}, state}
    end
  end

  def handle_call({:user_message, name, message}, state) do
    if machine_state = state[name] do
      private = machine_state.machine.handle_info(message, machine_state.private)

      {:noreply, Map.put(state, name, %{machine_state | private: private})}
    else
      {:noreply, state}
    end
  end

  def handle_call({:backup, name, path}, state) do
    if machine_state = state[name] do
      machine_state.backup(path, machine_state.private)

      {:reply, :ok, state}
    else
      {:reply, {:error, :unknown_group}, state}
    end
  end

  def handle_call({:restore, path}, state) do
    config =
      path
      |> Configuration.configuration_file()
      |> Configuration.read_file()

    if state[config.name] do
      raise "unable to restore, local member for group #{config.name} is running, you must first stop it with Craft.stop_member/1"
    end

    Configuration.delete_member_data(config.name)
    Configuration.restore_from_backup(path)

    {:reply, :ok}
  end

  defp init_machine(name, machine, state) do
    args =
      if machine.__craft_mutable__() do
        data_dir =
          name
          |> Configuration.find(namespace())
          |> Map.fetch!(:data_dir)

        data_dir = Path.join([Configuration.data_dir(), data_dir, "machine"])

        File.mkdir_p!(data_dir)

        %{name: name, data_dir: data_dir}
      else
        %{name: name}
      end

    {:ok, private} = machine.init(args)

    Map.put(state, name, %MachineState{module: machine, private: private})
  end

  defp namespace do
    self()
    |> :erlang.pid_to_list()
    |> :erlang.list_to_binary()
    |> Base.encode16()
  end

  defmodule Manager do
    @moduledoc false

    use GenServer

    import Craft.Application, only: [lookup: 2]

    def start_link(args) do
      GenServer.start_link(__MODULE__, args, name: __MODULE__)
    end

    defmodule State do
      defstruct [
        name_to_pids: %{},
        pid_to_name: %{}
      ]
    end

    def init(_) do
      {:ok, %State{}}
    end

    def handle_call({:join, name, pid}, _from, state) do
      if current = state.pid_to_name[pid] do
        {:reply, {:error, {:already_joined, current}}, state}
      else
        pids =
          case Map.fetch(state.name_to_pids, name) do
            {:ok, pids} ->
              MapSet.put(pids, pid)

            :error ->
              {:ok, _} = DynamicSupervisor.start_child(Craft.Supervisor, {Craft.Sandbox, name})

              MapSet.new([pid])
          end

        state =
          %{state |
            name_to_pids: Map.put(state.name_to_pids, name, pids),
            pid_to_name: Map.put(state.pid_to_name, pid, name)
          }

        Process.monitor(pid)
        {:reply, :ok, state}
      end
    end

    def handle_call({:find, pid}, _from, state) do
      {:reply, Map.fetch(state.pid_to_name, pid), state}
    end

    def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
      state =
        case Map.pop(state.pid_to_name, pid) do
          {nil, _} ->
            state

          {name, pid_to_name} ->
            {_, name_to_pids} =
              Map.get_and_update!(state.name_to_pids, name, fn pids ->
                if pids == MapSet.new([pid]) do
                  :ok = DynamicSupervisor.terminate_child(Craft.Supervisor, lookup(name, Craft.Sandbox))

                  :pop
                else
                  {nil, MapSet.delete(pids, pid)}
                end
              end)

            %{state | name_to_pids: name_to_pids, pid_to_name: pid_to_name}
        end

      {:noreply, state}
    end
  end
end
