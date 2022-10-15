defmodule Craft.TestHelper do
  alias Craft.Consensus

  def start_group(states, nodes) do
    name =
      :crypto.strong_rand_bytes(3)
      |> Base.encode16()

    states =
      Enum.zip_with(states, nodes, fn state, node ->
        {node, %{state | name: name, other_nodes: List.delete(nodes, node)}}
      end)

    for node <- nodes do
      :pong = Node.ping(node)
      {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    end

    # ensure all members are up and ready
    Task.async_stream(states, fn {node, state} ->
      :rpc.call(node, Craft.Application, :start_member, [state])
    end)
    |> Stream.run()

    Task.async_stream(nodes, fn node ->
      :gen_statem.cast({Consensus.name(name), node}, :run)
    end)
    |> Stream.run()

    :ok
  end
end

defmodule Craft.GroupWatcher do
  # alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  # alias Craft.Consensus.LeaderState
  alias Craft.RPC.AppendEntries

  defmodule State do
    defstruct [
      members: [],
      term: -1,
      leader: nil,
      empty_append_entries_counts: %{}, # num of consecutive successfully received and responded-to empty AppendEntries msgs
      genstatem_invocations: []
    ]

    def new(members) do
      %__MODULE__{members: members}
    end

    def leader_elected(%State{term: term} = state, leader, new_term) when new_term == term + 1 do
      empty_append_entries_counts =
        state.members
        |> List.delete(leader)
        |> Enum.into(%{}, & {&1, {0, :confirmed}})

      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts, leader: leader, term: new_term}
    end

    def append_entries_received(%State{term: term, leader: leader} = state, member, %AppendEntries{term: term, leader_id: leader} = append_entries) do
      IO.inspect member
      empty_append_entries_counts =
        if append_entries.entries == [] do
          Map.update!(state.empty_append_entries_counts, member, fn {num, :confirmed} -> {num, :received} end)
        else
          Map.put(state.empty_append_entries_counts, member, {0, :confirmed})
        end

      IO.inspect empty_append_entries_counts, label: :append_entries
      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts}
    end

    def append_entries_results_received(%State{term: term, leader: leader} = state, leader, %AppendEntries.Results{term: term} = append_entries_results) do
      IO.inspect append_entries_results
      empty_append_entries_counts =
        if append_entries_results.success do
          Map.update!(state.empty_append_entries_counts, append_entries_results.from, fn
            {num, :received} -> {num+1, :confirmed}
            _ ->
        Enum.sort_by(state.genstatem_invocations, fn {time, _} -> time end)
              |>
              Enum.each(&IO.inspect/1)
              raise "shit"
          end)
        else
          Map.put(state.empty_append_entries_counts, append_entries_results.from, {0, :confirmed})
        end

      IO.inspect empty_append_entries_counts, label: :append_entries_results
      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts}
    end
  end

  # @spec watch(member_nodes, until :: {:millisecs, pos_integer()}) |
  def watch(members, until \\ :leader_stable)
  def watch(members, millisecs: millisecs) do
    Process.send_after(self(), :stop_watching, millisecs)

    do_watch(State.new(members), nil)
  end
  def watch(members, until), do: do_watch(State.new(members), until)

  defp do_watch(state, until) do
    receive do
      :stop_watching ->
        invocations = Enum.sort_by(state.genstatem_invocations, fn {time, _} -> time end)
        {:ok, %State{state | genstatem_invocations: invocations}}

      {:trace, member_pid, :call, {Craft.Consensus, _fsm_state, [:cast, %AppendEntries{} = append_entries, _member_state]}} = invocation ->
        state
        |> record_invocation(invocation)
        |> State.append_entries_received(node(member_pid), append_entries)
        |> do_watch(until)

      {:trace, leader_pid, :call, {Craft.Consensus, _fsm_state, [:cast, %AppendEntries.Results{} = append_entries_results, _member_state]}} = invocation ->
        state =
          state
          |> record_invocation(invocation)
          |> State.append_entries_results_received(node(leader_pid), append_entries_results)

        if until == :leader_stable do
          IO.inspect state.empty_append_entries_counts
        end

        do_watch(state, until)

      {:trace, leader_pid, :call, {Craft.Consensus, :leader, [:enter, :candidate, %CandidateState{current_term: current_term}]}} = invocation ->
        state
        |> record_invocation(invocation)
        |> State.leader_elected(node(leader_pid), current_term)
        |> do_watch(until)

      # {:trace, _, _, _} = invocation ->
      {:trace, _, _, _} = invocation ->
        state
        |> record_invocation(invocation)
        |> do_watch(until)
    end
  end

  defp record_invocation(state, invocation) do
    invocation = {DateTime.utc_now(), invocation}
    %State{state | genstatem_invocations: [invocation | state.genstatem_invocations]}
  end
end


ExUnit.start()
