defmodule Craft.MemberSupervisor do
  @moduledoc false

  alias Craft.Persistence.RocksDBPersistence

  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: name(args.name))
  end

  @impl Supervisor
  def init(args) do
    {consensus_args, machine_args} =
      case args do
        # for testing
        %{consensus_state: consensus_args, machine_args: machine_args} ->
          {consensus_args, machine_args}

        _ ->
          {args, args}
      end

    children = [
      {Craft.Consensus, [consensus_args]},
      {Craft.Machine, machine_args}
      # {Registry, keys: :unique, name: registry_name(args.name)},
      # {ARQ, name: rpc_supervisor_name(args.name)}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # TODO: - :data_dir implies {RocksDBPersistence, data_dir: data_dir}
  #       - allow passing :persistence key has a {module, args} tuple
  def start_member(name, nodes, machine, opts \\ []) do
    args =
      opts
      |> Map.new()
      |> Map.put(:name, name)
      |> Map.put(:nodes, nodes)
      |> Map.put(:machine, machine)
      |> Map.put(:persistence, {RocksDBPersistence, []})

    DynamicSupervisor.start_child(Craft.Supervisor, {__MODULE__, args})
  end

  def stop_member(name) do
    pid =
      name
      |> name()
      |> Process.whereis()

    DynamicSupervisor.terminate_child(Craft.Supervisor, pid)
  end

  def name(name) do
    Module.concat(__MODULE__, name)
  end

  # def registry_name(name) do
  #   Module.concat(Craft.Registry, name)
  # end

  # def rpc_supervisor_name(name) do
  #   Module.concat(Craft.Registry, name)
  # end
end
