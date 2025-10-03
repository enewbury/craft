# manages locally persistent configurations
#
# not super in love with this approach, it doesn't scale very well, but it avoids a centralized db, so it should make backups easier
#
# :erlang.term_to_binary/2 isn't guaranteed to be consistent over OTP releases, but :dets uses it, so i guess it's actually stable?
#
#
defmodule Craft.Configuration do
  @moduledoc false

  def find(name) do
    with {dir, file} <- find_file(name) do
      file
      |> File.read!()
      |> :erlang.binary_to_term()
      |> Map.put(:data_dir, dir)
    end
  end

  def copy_configuration(name, to_directory) do
    with {_dir, file} <- find_file(name) do
      to_file = configuration_file(to_directory)

      File.cp(file, to_file)
    end
  end

  def delete_member_data(name) do
    with {dir, _file} <- find_file(name) do
      data_dir()
      |> Path.join(dir)
      |> File.rm_rf!()
    end
  end

  def restore_from_backup(path) do
    config =
      path
      |> configuration_file()
      |> read_file()

    new_path = make_new_directory(config.name)

    File.cp_r!(path, new_path)
  end

  defp find_file(name) do
    data_dir()
    |> File.ls!()
    |> Enum.find_value(fn dir ->
      path = Path.join(data_dir(), dir)

      if File.dir?(path) do
        config_file = configuration_file(path)

        config = read_file(config_file)

        if config.name == name do
          {dir, config_file}
        end
      end
    end)
  end

  def read_file(file) do
    file
    |> File.read!()
    |> :erlang.binary_to_term()
  end

  def configuration_file(dir) do
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

    name
    |> make_new_directory()
    |> configuration_file()
    |> File.write!(contents)

    find(name)
  end

  def make_new_directory(name) do
    hash =
      name
      |> :erlang.phash2()
      |> Integer.to_string()

    path = Path.join(data_dir(), hash)

    if File.exists?(path) do
      :crypto.strong_rand_bytes(16)
      |> Base.encode16(case: :lower)
      |> make_new_directory()
    else
      File.mkdir_p!(path)

      path
    end
  end

  def data_dir do
    Application.get_env(:craft, :data_dir)
  end
end
