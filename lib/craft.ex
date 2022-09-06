defmodule Craft do
  alias Craft.Consensus
  alias Craft.Log.MapLog

  def start_group(name, nodes, opts \\ [log_module: MapLog]) do
    for node <- nodes do
      :pong = Node.ping(node)
      {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    end

    for node <- nodes do
      {:ok, _pid} = :rpc.call(node, Craft, :start_member, [name, nodes, opts])
    end
  end

  defdelegate start_member(name, nodes, opts), to: Craft.Application

  def step_down(name, node) do
    :gen_statem.cast({Consensus.name(name), node}, :step_down)
  end

  # def add_member
  # def command
  # def query

  defdelegate start_dev_test_cluster(num \\ 5), to: Craft.Test.ClusterNodes, as: :spawn_nodes

  def start_dev_consensus_group(nodes) do
    :crypto.strong_rand_bytes(3)
    |> Base.encode16()
    |> start_group(nodes)

    :ok
  end
end
