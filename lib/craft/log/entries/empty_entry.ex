# used to start a fresh log, or appended when a new leader is elected
defmodule Craft.Log.EmptyEntry do
  defstruct [:term]
end
