defmodule Craft.Machine do
  @type private :: any()

  @callback init(Craft.group_name()) :: {:ok, state :: any()}
  @callback command(Craft.command(), Craft.log_index(), state :: any()) :: {Craft.reply(), state :: any()} | {Craft.reply(), Craft.side_effects(), state :: any()}
  @callback last_applied_log_index(state :: any()) :: Craft.log_index()

  defmodule State do
    defstruct [
      :private,
      last_applied: 0
    ]
  end

  def __using__(opts) do
    quote do
      if !Keyword.fetch!(unquote(opts), :persistent) do
        def last_applied_log_index(state = %State{}), do: state.last_applied
      end
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      defoverridable init: 1, command: 3

      def init(group_name) do
        {:ok, private} = super(group_name)

        %State{private: private}
      end

      def command(command, log_index, %State{} = state) do
        case super(command, log_index, state.private) do
          {reply, private} ->
            {reply, %State{state | private: private, last_applied: log_index}}

          {reply, side_effects, private} ->
            {reply, side_effects, %State{state | private: private, last_applied: log_index}}
        end
      end
    end
  end
end
