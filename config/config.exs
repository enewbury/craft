import Config

config :logger,
  handle_otp_reports: true,
  handle_sasl_reports: true

config :logger, :console,
  level: :info,
  format: "[$level] $metadata$message\n",
  metadata: [:name, :t, :node, :term]

#import_config "#{config_env()}.exs"
