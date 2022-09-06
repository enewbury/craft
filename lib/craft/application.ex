defmodule Craft.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    if Mix.env() == :dev do
      Logger.add_translator({Craft.SASLLoggerTranslator, :translate})
    end

    DynamicSupervisor.start_link([strategy: :one_for_one, name: Craft.Supervisor])
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
