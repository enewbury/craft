defmodule ExplicitSandboxTest do
  use ExUnit.Case

  setup do
    name = {:hi, "i'm", :a, "sandbox", make_ref()}

    on_exit fn ->
      Craft.Sandbox.stop(name)
    end

    [name: name]
  end

  test "requires an explicit join to a sandbox" do
    error =
      assert_raise RuntimeError, fn ->
        Craft.start_group("abc", [], Craft.SandboxTestMachine)
      end

    assert error.message =~ ~r/^no sandbox configured/
  end

  test "join", ctx do
    Craft.Sandbox.join(ctx.name)

    assert :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)
    assert :ok = Craft.command({:put, "a", "b"}, "abc")
    assert {:ok, "b"} = Craft.query({:get, "a"}, "abc")
  end

  test "other process joins", ctx do
    me = self()

    pid =
      spawn_link(fn ->
        receive do
          :do_put ->
            assert :ok = Craft.command({:put, "a", "b"}, "abc")
            send(me, :do_get)
        end
      end)

    Craft.Sandbox.join(ctx.name)
    Craft.Sandbox.join(ctx.name, pid)

    assert :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)

    send(pid, :do_put)

    receive do
      :do_get ->
        assert {:ok, "b"} = Craft.query({:get, "a"}, "abc")
    end
  end

  test "no inheritance", ctx do
    Craft.Sandbox.join(ctx.name)

    assert :ok = Craft.start_group("abc", [], Craft.SandboxTestMachine)
    assert :ok = Craft.command({:put, "a", "b"}, "abc")
    assert {:error, %RuntimeError{message: "no sandbox" <> _}} = Craft.query({:get_parallel, "a"}, "abc")
  end
end
