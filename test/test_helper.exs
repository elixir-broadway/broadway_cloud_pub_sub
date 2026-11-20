# The gRPC client logs connection lifecycle at :debug; it is noise in test output.
Logger.configure(level: :info)

ExUnit.start()
