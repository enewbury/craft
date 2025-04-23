defmodule Craft.MemberSupervisor do
  @moduledoc false

  use Supervisor

  alias Craft.Persistence.RocksDBPersistence

  import Craft.Application, only: [via: 2, lookup: 2]

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: via(args.name, __MODULE__))
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
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # TODO: - :data_dir implies {RocksDBPersistence, data_dir: data_dir}
  #       - allow passing :persistence key has a {module, args} tuple
  def start_member(name, nodes, machine, opts \\ []) do
    args =
      [
        name: name,
        nodes: nodes,
        machine: machine,
        persistence: {RocksDBPersistence, []}
      ]
      |> Keyword.merge(opts)
      |> Map.new()

    DynamicSupervisor.start_child(Craft.Supervisor, {__MODULE__, args})
  end

  def stop_member(name) do
    DynamicSupervisor.terminate_child(Craft.Supervisor, lookup(name, __MODULE__))
  end
end
