import Config

config :logger,
  handle_otp_reports: true,
  handle_sasl_reports: true

config :logger, :console,
  level: :info,
  format: "[$level] $metadata$message\n",
  metadata: [:name, :t, :term, :node]

#import_config "#{config_env()}.exs"
