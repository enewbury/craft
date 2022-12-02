defmodule Craft.MemberSupervisor do
  @moduledoc false

  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: Module.concat(__MODULE__, args.name))
  end

  @impl Supervisor
  def init(args) do
    args
    |> Map.delete(:nodes)
    |> Map.put(:other_nodes, List.delete(args.nodes, node()))
    |> do_init()
  end

  if Mix.env() == :test do
    defoverridable init: 1
    def init(state) do
      do_init(state)
    end
  end

  defp do_init(args) do
    children = [
      {Craft.Consensus, [args]},
      {Craft.Machine, [args]}
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
