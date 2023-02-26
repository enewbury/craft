defmodule Craft.Log.NewConfigurationEntry do
  # @type t :: %__MODULE__{
  #   term: integer(),
  #   configuration: Craft.Consensus.State.Configuration.t()
  # }

  defstruct [
    :term,
    :configuration
  ]
end
