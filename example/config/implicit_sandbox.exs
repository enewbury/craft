import Config

config :craft, :backend, {Craft.Sandbox, lookup: {ImplicitSandboxTest, :lookup}}
