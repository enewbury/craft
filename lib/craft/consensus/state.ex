defmodule Craft.Consensus.State do
  defstruct [
    :name,
    :other_nodes,
    :log,
    :nexus_pid,
    :leader_id,
    {:current_term, -1},
    {:commit_index, 0},

    :mode_state
  ]
end
