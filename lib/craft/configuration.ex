# manages locally persistent configurations
#
# not super in love with this approach, it doesn't scale very well, but it avoids a centralized db, so it should make backups easier
#
# :erlang.term_to_binary/2 isn't guaranteed to be consistent over OTP releases, but :dets uses it, so i guess it's actually stable?
#
defmodule Craft.Configuration do
  @moduledoc false

  def find(name) do
    data_dir()
    |> File.ls!()
    |> Enum.find_value(fn dir ->
      path = Path.join(data_dir(), dir)

      if File.dir?(path) do
        config =
          path
          |> configuration_file()
          |> File.read!()
          |> :erlang.binary_to_term()

        if config.name == name do
          Map.put(config, :data_dir, dir)
        end
      end
    end)
  end

  defp configuration_file(dir) do
    Path.join(dir, "config.erlterm")
  end

  # def update!(config, key, value) do
  #   if config = find(name) do
  #     contents =
  #       config
  #       |> Map.delete(:data_dir)
  #       |> Map.put(key, value)
  #       |> :erlang.term_to_binary()

  #     config.data_dir
  #     |> configuration_file()
  #     |> File.write!(contents)

  #   else
  #     raise "attempted to update missing config for group #{inspect name}"
  #   end
  # end

  def write_new!(name, config) when is_map(config) do
    contents =
      config
      |> Map.put(:name, name)
      |> :erlang.term_to_binary()

    make_new_directory()
    |> configuration_file()
    |> File.write!(contents)

    find(name)
  end

  def make_new_directory do
    name = random_string()
    path = Path.join(data_dir(), name)

    if File.exists?(path) do
      make_new_directory()
    else
      File.mkdir_p!(path)

      path
    end
  end

  def data_dir do
    Application.get_env(:craft, :data_dir)
  end

  defp random_string do
    :crypto.strong_rand_bytes(16)
    |> Base.encode16(case: :lower)
  end
end
