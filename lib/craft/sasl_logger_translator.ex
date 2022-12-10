defmodule Craft.SASLLoggerTranslator do
  @moduledoc false

  # filter out SASL info/debug messages in dev

  def translate(_, :debug, _, _), do: :skip
  def translate(_, :info, _, _), do: :skip
  def translate(_, _, _, _), do: :none
end
