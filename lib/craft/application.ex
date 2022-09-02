defmodule Craft.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    DynamicSupervisor.start_link([strategy: :one_for_one, name: Craft.Supervisor])
  end

  def start_member(name, nodes) do
    DynamicSupervisor.start_child(Craft.Supervisor, {Craft.MemberSupervisor, %{name: name, nodes: nodes}})
  end
end
