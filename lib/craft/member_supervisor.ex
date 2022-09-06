defmodule Craft.MemberSupervisor do
  @moduledoc false

  use Supervisor

  def start_link(%{name: name} = args) do
    Supervisor.start_link(__MODULE__, args, name: Module.concat(__MODULE__, name))
  end

  @impl Supervisor
  def init(%{name: name, nodes: nodes, log_module: log_module}) do
    other_nodes = List.delete(nodes, node())

    children = [
      {Craft.Consensus, [name, other_nodes, log_module]},
      {Registry, keys: :unique, name: registry_name(name)},
      {ARQ, name: rpc_supervisor_name(name)}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def registry_name(name) do
    Module.concat(Craft.Registry, name)
  end

  def rpc_supervisor_name(name) do
    Module.concat(Craft.Registry, name)
  end
end
