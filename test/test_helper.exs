# The gRPC client logs connection lifecycle at :debug; it is noise in test output.
Logger.configure(level: :info)

# Producers read PUBSUB_EMULATOR_HOST to choose their defaults. Clear it so a
# developer's shell does not change what the tests see; tests that need it set
# it themselves.
System.delete_env("PUBSUB_EMULATOR_HOST")

ExUnit.start()
