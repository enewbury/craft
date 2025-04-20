defmodule Craft.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    if Mix.env() in [:dev, :test] do
      Logger.add_translator({Craft.SASLLoggerTranslator, :translate})
    end

    children = [
      {DynamicSupervisor, [strategy: :one_for_one, name: Craft.Supervisor]},
      {Registry, keys: :unique, name: Craft.Registry},
      Craft.MemberCache
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  def via(name, component) do
    {:via, Registry, {Craft.Registry, {name, component}}}
  end

  def lookup(name, component) do
    [{pid, _meta}] = Registry.lookup(Craft.Registry, {name, component})

    pid
  end
end
