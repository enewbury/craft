defmodule Craft.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    if Mix.env() in [:dev, :test] do
      Logger.add_translator({Craft.SASLLoggerTranslator, :translate})
    end

    Craft.LeaderCache.init()

    DynamicSupervisor.start_link([strategy: :one_for_one, name: Craft.Supervisor])
  end

  if Mix.env() == :test do
    def start_member(consensus_state, machine_args) do
      DynamicSupervisor.start_child(Craft.Supervisor, {Craft.MemberSupervisor, [consensus_state, machine_args]})
    end
  end

  def start_member(name, nodes, machine, opts) do
    args =
      opts
      |> Map.new()
      |> Map.put(:name, name)
      |> Map.put(:nodes, nodes)
      |> Map.put(:machine, machine)

    DynamicSupervisor.start_child(Craft.Supervisor, {Craft.MemberSupervisor, args})
  end
end
