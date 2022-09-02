import Config

config :logger, :console,
  level: :info,
  format: "$date $time [$level] $metadata$message\n",
  metadata: [:name, :node, :term, :state, :timeout]

#import_config "#{config_env()}.exs"
