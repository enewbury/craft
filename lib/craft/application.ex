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
end
