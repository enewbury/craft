defmodule Craft.MemberSupervisor do
  @moduledoc false

  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: Module.concat(__MODULE__, args.name))
  end

  @impl Supervisor
  def init(args) do
    args =
      args
      |> Map.delete(:nodes)
      |> Map.put(:other_nodes, List.delete(args.nodes, node()))

    {consensus_args, machine_args} =
      case args do
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

  # def registry_name(name) do
  #   Module.concat(Craft.Registry, name)
  # end

  # def rpc_supervisor_name(name) do
  #   Module.concat(Craft.Registry, name)
  # end
end
