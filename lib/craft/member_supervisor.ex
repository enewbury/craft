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

    do_init(args, args)
  end

  if Mix.env() == :test do
    defoverridable init: 1
    def init(consensus_state, machine_args) do
      do_init(consensus_state, machine_args)
    end
  end

  defp do_init(consensus_args, machine_args) do
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
