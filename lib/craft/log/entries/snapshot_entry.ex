defmodule Craft.Log.SnapshotEntry do
  #
  # this type of entry is never replicated to other members, it's just a stub prepended
  # to the log after it's been truncated post-snapshot
  #
  defstruct [:term]
end
