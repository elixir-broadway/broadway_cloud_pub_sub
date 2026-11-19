# Using the Pub/Sub emulator

Both producers work with the local
[Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator). Neither
reads the `PUBSUB_EMULATOR_HOST` environment variable on its own, so point the
producer at the emulator with options and replace the default Goth token
generator, since the emulator does not check credentials.

The examples assume the emulator listens on `localhost:8085`, for example
started with `gcloud beta emulators pubsub start --host-port=localhost:8085`.

## Token generator

Any token is accepted, so a generator that returns a fixed string is enough and
Goth is not needed:

```elixir
defmodule MyApp do
  def emulator_token, do: {:ok, "emulator"}
end
```

## Streaming producer

`BroadwayCloudPubSub.Producer` connects over gRPC. Give it the emulator's
`host:port` and turn off TLS, since the emulator does not use it:

```elixir
{BroadwayCloudPubSub.Producer,
 subscription: "projects/my-project/subscriptions/my-subscription",
 grpc_endpoint: "localhost:8085",
 use_ssl: false,
 token_generator: {MyApp, :emulator_token, []}}
```

The streaming producer still needs an HTTP/2 adapter in your dependencies (see
the README).

## Pull producer

`BroadwayCloudPubSub.Pull.Producer` uses HTTP. Its `:base_url` takes a URL, so
include the `http://` scheme:

```elixir
{BroadwayCloudPubSub.Pull.Producer,
 subscription: "projects/my-project/subscriptions/my-subscription",
 base_url: "http://localhost:8085",
 token_generator: {MyApp, :emulator_token, []}}
```

## Switching on `PUBSUB_EMULATOR_HOST`

To follow the convention of Google's client libraries, choose the options where
the pipeline is started. Like those libraries, treat an empty value the same as
an unset one:

```elixir
defmodule MyApp.PubSub do
  def producer_options(subscription) do
    case System.get_env("PUBSUB_EMULATOR_HOST") do
      host when host in [nil, ""] ->
        [subscription: subscription, goth: MyApp.Goth]

      host ->
        [
          subscription: subscription,
          grpc_endpoint: host,
          use_ssl: false,
          token_generator: {MyApp, :emulator_token, []}
        ]
    end
  end
end
```

Then pass them to the producer:

```elixir
producer: [
  module:
    {BroadwayCloudPubSub.Producer,
     MyApp.PubSub.producer_options("projects/my-project/subscriptions/my-subscription")}
]
```

For the pull producer, use `base_url: "http://" <> host` instead of
`:grpc_endpoint` and `:use_ssl`.

## Things to know

- **The emulator starts empty.** Create the topic and subscription before
  starting the pipeline. The emulator accepts any project name. With `curl`:

  ```shell
  curl -X PUT http://localhost:8085/v1/projects/my-project/topics/my-topic
  curl -X PUT http://localhost:8085/v1/projects/my-project/subscriptions/my-subscription \
    -H "content-type: application/json" \
    -d '{"topic": "projects/my-project/topics/my-topic"}'
  ```

- **Topics and subscriptions live in memory.** Restarting the emulator
  removes them, so create them again after every restart.

- **A missing subscription stops the streaming producer.** The emulator
  answers `NOT_FOUND`, which the streaming producer treats as a terminal error
  (see the "Error handling" section of `BroadwayCloudPubSub.Producer`). It
  stops, Broadway restarts it, and the restarts fail immediately, so Broadway's
  restart limit is reached within a second and the whole pipeline exits. Under
  a default application supervisor this can stop the application. Create the
  subscription before starting the pipeline. The pull producer logs the error
  on each request and keeps polling.

- **To publish a test message**, the emulator's REST API takes base64 data:

  ```shell
  curl -X POST http://localhost:8085/v1/projects/my-project/topics/my-topic:publish \
    -H "content-type: application/json" \
    -d "{\"messages\": [{\"data\": \"$(printf 'hello' | base64 | tr -d '\n')\"}]}"
  ```
