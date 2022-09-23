defmodule Craft.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    if Mix.env() in [:dev, :test] do
      Logger.add_translator({Craft.SASLLoggerTranslator, :translate})
    end

    DynamicSupervisor.start_link([strategy: :one_for_one, name: Craft.Supervisor])
  end

  if Mix.env() == :test do
    def start_member(state) do
      DynamicSupervisor.start_child(Craft.Supervisor, {Craft.MemberSupervisor, state})
    end
  end

  def start_member(name, nodes, opts) do
    args =
      opts
      |> Map.new()
      |> Map.put(:name, name)
      |> Map.put(:nodes, nodes)

    DynamicSupervisor.start_child(Craft.Supervisor, {Craft.MemberSupervisor, args})
  end
end
