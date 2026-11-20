# Using the Pub/Sub emulator

Both producers work with the local
[Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator).

The examples assume the emulator listens on `localhost:8085`, for example
started with `gcloud beta emulators pubsub start --host-port=localhost:8085`.

## `PUBSUB_EMULATOR_HOST`

Like Google's client libraries, both producers read the `PUBSUB_EMULATOR_HOST`
environment variable when the pipeline starts. When it is set, for example with
`export PUBSUB_EMULATOR_HOST=localhost:8085`, they connect to the emulator at
that address without TLS and use a placeholder token instead of Goth, since the
emulator does not check credentials. An empty value counts as unset.

Options you pass are kept, so the variable only fills in the ones you leave
out: `:grpc_endpoint`, `:use_ssl` and `:token_generator` for
`BroadwayCloudPubSub.Producer`, and `:base_url` and `:token_generator` for
`BroadwayCloudPubSub.Pull.Producer`. A `:goth` option is ignored while the
variable is set, so the same pipeline configuration works against the emulator
and against Google Cloud.

The rest of this guide configures the emulator with options instead, for when
the environment variable is not convenient.

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
