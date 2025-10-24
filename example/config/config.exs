import Config

# config :logger,
#   handle_otp_reports: true,
#   handle_sasl_reports: true

# config :logger, :default_handler,
#   level: :info

# config :logger, :default_formatter,
#   format: "[$level] $metadata\t| $message\n",
#   metadata: [:name, :t, :term, :node]

# config :craft, :snapshot_server_port, 1337

config :craft, :data_dir, "data"

# if config_env() == :test do
#   config :craft, :logger, [{:handler, :nexus_handler, Craft.Nexus, %{}}]
# end

import_config "#{config_env()}.exs"
