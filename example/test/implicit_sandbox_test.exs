defmodule ImplicitSandboxTest do
  use ExUnit.Case

  setup_all do
    {:ok, _} = Agent.start_link(fn -> %{} end, name: __MODULE__)

    :ok
  end

  test "sandbox determined via configured lookup function" do
    name = {:hi, "i'm", :a, "sandbox", make_ref()}

    me = self()
    Agent.update(__MODULE__, &Map.put(&1, me, name))

    assert :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)
    assert :ok = Craft.command({:put, "a", "b"}, "abc")
    assert {:ok, "b"} = Craft.query({:get, "a"}, "abc")
  end

  test "other process looks up different sandbox" do
    name = {:hi, "i'm", :a, "sandbox", make_ref()}

    me = self()
    Agent.update(__MODULE__, &Map.put(&1, me, name))
    assert :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)
    assert :ok = Craft.command({:put, "a", "b"}, "abc")

    pid =
      spawn_link(fn ->
        receive do
          :go ->
            :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)
            :ok = Craft.command({:put, "a", "not b"}, "abc")
            result = Craft.query({:get, "a"}, "abc")

            send(me, {:result, result})
        end
      end)

    name = :some_other_sandbox
    Agent.update(__MODULE__, &Map.put(&1, pid, name))

    send(pid, :go)

    receive do
      {:result, result} ->
        assert {:ok, "not b"} = result
    end

    assert {:ok, "b"} = Craft.query({:get, "a"}, "abc")
  end

  def lookup(pid) do
    Agent.get(__MODULE__, &Map.fetch(&1, pid))
  end
end
