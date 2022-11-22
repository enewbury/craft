defmodule Craft.Machine do
  #FIXME
  @type command :: any()
  @type state :: any()
  @type reply :: any()
  @type side_effects :: any()
  @type group_name :: any()
  @type query() :: any()

  @callback init(group_name()) :: {:ok, state()}
  @callback command(command(), state()) :: {reply(), state()} | {reply(), side_effects(), state()}
  # @callback query(query(), state()) :: {reply(), state()}

  # @optional_callbacks query
end
