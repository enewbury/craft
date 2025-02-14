defmodule Craft.TestCluster do
  @moduledoc false

  defmacro remote_run(node, args \\ [], timeout \\ 1000, do: block) do
    quote do
      pid = unquote(node).pid
      block = unquote(Macro.escape(block))
      {result, _binding} = :peer.call(pid, Code, :eval_quoted, [block, unquote(args)], unquote(timeout))
      result
    end
  end

  def spawn_nodes(num) do
    {:ok, _} = Node.start(:primary, :shortnames)

    1..num |> Enum.reduce([], fn _, nodes -> [spawn_node(nodes) | nodes] end) |> Enum.reverse()
  end

  def apply(node, module, fun, args) do
    :peer.call(node.pid, module, fun, args)
  end

  defp spawn_node(nodes) do
    {:ok, pid, node} = :peer.start_link(%{name: :peer.random_name(), connection: :standard_io})

    :peer.call(pid, :code, :add_paths, [:code.get_path()])
    connect_to_cluster(pid, nodes)
    transfer_config(pid)
    start_apps(pid)

    %{node: node, pid: pid}
  end

  defp connect_to_cluster(pid, [%{node: last_node} | _]) do
    :peer.call(pid, :net_kernel, :connect_node, [last_node])
  end

  defp connect_to_cluster(_pid, []), do: :ok

  defp transfer_config(pid) do
    Application.loaded_applications()
    |> Enum.map(fn {app_name, _, _} -> app_name end)
    |> Enum.map(fn app_name -> {app_name, Application.get_all_env(app_name)} end)
    |> Enum.each(fn {app_name, env} ->
      Enum.each(env, fn {key, val} ->
        :ok = :peer.call(pid, Application, :put_env, [app_name, key, val, [persistent: true]])
      end)
    end)

    # :peer's remote nodes don't support ExUnit's :capture_log from nonode@nohost
    # this configures Logger in remote nodes to be enough close in behavior to :capture_log,
    # or if not configured, it uses Logger.level as fallback
    logger_level =
      case ExUnit.configuration()[:capture_log] do
        [level: level] ->
          case level do
            :emergency -> :none
            :alert -> :emergency
            :critical -> :alert
            :error -> :critical
            :warning -> :error
            :notice -> :warning
            :info -> :notice
            :debug -> :info
            _ -> :debug
          end

        true ->
          :none

        _ ->
          Logger.level()
      end

    :peer.call(pid, Application, :put_env, [:logger, :level, logger_level, [persistent: true]])
  end

  defp start_apps(pid) do
    :peer.call(pid, Application, :ensure_all_started, [:mix])
    :peer.call(pid, Mix, :env, [Mix.env()])
    :peer.call(pid, Application, :ensure_all_started, [:craft], 20_000)
  end
end
