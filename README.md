# BroadwayCloudPubSub

[![CI](https://github.com/dashbitco/broadway_cloud_pub_sub/actions/workflows/ci.yml/badge.svg)](https://github.com/dashbitco/broadway_cloud_pub_sub/actions/workflows/ci.yml)

A Google Cloud Pub/Sub connector for [Broadway](https://github.com/dashbitco/broadway).

Documentation can be found at [https://hexdocs.pm/broadway_cloud_pub_sub](https://hexdocs.pm/broadway_cloud_pub_sub).

## What's in the box

* `BroadwayCloudPubSub.Producer`: Broadway producer using the gRPC
  [StreamingPull][gcp-streamingpull] API. Messages are pushed by the server over
  a persistent bidirectional stream, giving low latency and high throughput with
  automatic lease extension and server-side flow control. **This is the
  recommended producer**, in line with Google's own [guidance][gcp-streamingpull]
  that StreamingPull is what their first-party client libraries use "where
  possible".
* `BroadwayCloudPubSub.Pull.Producer`: Broadway producer using the unary HTTP
  [Pull][gcp-pull-api] API. Retained for environments where gRPC is unavailable
  or undesired, and for the cases Google lists as Pull-only: when you need
  strict control over the number of messages pulled per request, tight control
  over client memory and CPU, or when your subscriber acts as a proxy to
  another pull-oriented system.
* `BroadwayCloudPubSub.Streaming.Client`: Behaviour for custom gRPC client implementations.
* `BroadwayCloudPubSub.Pull.Client`: Behaviour for custom HTTP pull client implementations.

[gcp-streamingpull]: https://cloud.google.com/pubsub/docs/pull#streamingpull_api
[gcp-pull-api]: https://cloud.google.com/pubsub/docs/pull#pull_api

## Installation

Add `:broadway_cloud_pub_sub` to your dependencies, along with an HTTP/2 adapter
for `:grpc`:

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 2.0"},
    {:goth, "~> 1.3"},
    {:grpc, "~> 1.0"},
    {:protobuf, "~> 0.12"},
    # Pick one HTTP/2 adapter:
    {:gun, "~> 2.2"},
    # or
    # {:mint, "~> 1.9"},
    # {:castore, "~> 1.0"}
  ]
end
```

> The [goth](https://hexdocs.pm/goth) package handles Google Authentication and
> is required for the default token generator.
>
> The `grpc` and `protobuf` packages are required by
> `BroadwayCloudPubSub.Producer`. You must pick one HTTP/2 adapter for the gRPC
> connection and add it to your `mix.exs`: either `:gun`, or `:mint` together
> with `:castore`.
>
> If you only use `BroadwayCloudPubSub.Pull.Producer` you may omit `:grpc`,
> `:protobuf`, and the adapter packages.

## Usage

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Producer,
      goth: MyGoth,
      subscription: "projects/my-project/subscriptions/my-subscription",
      max_outstanding_messages: 1000
    }
  ],
  processors: [default: [concurrency: 10]]
)
```

See `BroadwayCloudPubSub.Producer` for the full option reference, including flow
control, reconnection backoff, graceful shutdown, and telemetry.

### HTTP/2 adapter

The producer supports two adapters. Both are optional dependencies of `:grpc`,
so you select one by adding it to your application's `mix.exs` (see
[Installation](#installation)).

- `:gun` (default): [Gun](https://github.com/ninenines/gun) HTTP/2 client.
  Add `{:gun, "~> 2.2"}` to your deps.
- `:mint`: [Mint](https://github.com/elixir-mint/mint) HTTP/2 client.
  Add `{:mint, "~> 1.9"}` and `{:castore, "~> 1.0"}` to your deps.

Then select the adapter in your producer config:

```elixir
{BroadwayCloudPubSub.Producer,
 goth: MyGoth,
 subscription: "projects/my-project/subscriptions/my-subscription",
 adapter: :mint}
```

### Using the HTTP pull producer

If gRPC is not available in your environment or you prefer to use the HTTP pull method, use `BroadwayCloudPubSub.Pull.Producer`:

```elixir
Broadway.start_link(MyBroadway,
  name: MyBroadway,
  producer: [
    module: {BroadwayCloudPubSub.Pull.Producer,
      goth: MyGoth,
      subscription: "projects/my-project/subscriptions/my-subscription"
    }
  ],
  processors: [default: [concurrency: 10]]
)
```

### Upgrading from 1.x

> **2.0 is a major release with breaking changes.** The three-line summary is
> below; the [full upgrade guide](docs/upgrade_to_2.0.md) has step-by-step
> instructions, option mapping tables, and rationale for every change.

#### Breaking change 1: [`BroadwayCloudPubSub.Producer` is now the gRPC streaming producer](docs/upgrade_to_2.0.md#1-new-default-producer)

The biggest change: the module name `BroadwayCloudPubSub.Producer` now refers to
the **new gRPC StreamingPull producer**. The 1.x HTTP pull producer lives on
under `BroadwayCloudPubSub.Pull.Producer`.

```elixir
# 1.x — HTTP pull producer
{BroadwayCloudPubSub.Producer, goth: MyApp.Goth, subscription: "..."}

# 2.0 option A — switch to streaming (recommended, lower latency)
{BroadwayCloudPubSub.Producer,
 goth: MyApp.Goth,
 subscription: "...",
 max_outstanding_messages: 1000}

# 2.0 option B — keep HTTP pull, one-line change
{BroadwayCloudPubSub.Pull.Producer, goth: MyApp.Goth, subscription: "..."}
```

The streaming producer requires `:grpc`, `:protobuf`, and an HTTP/2 adapter
(`:gun` or `:mint` + `:castore`). If you stay on the pull producer those
packages are not needed.

#### Breaking change 2: [two modules renamed](docs/upgrade_to_2.0.md#2-broadwaycloudpubsubpullclient-renamed) (only if referenced directly)

| 1.x | 2.0 |
|---|---|
| `BroadwayCloudPubSub.PullClient` | `BroadwayCloudPubSub.Pull.FinchClient` |
| `BroadwayCloudPubSub.Client` (behaviour) | `BroadwayCloudPubSub.Pull.Client` |

These only matter if you passed the module explicitly (e.g. `client:
BroadwayCloudPubSub.PullClient`) or implemented a custom pull client with
`@behaviour BroadwayCloudPubSub.Client`.

#### Breaking change 3: [`on_failure` default changed from `:noop` to `{:nack, 0}`](docs/upgrade_to_2.0.md#4-on_failure-default-changed-noop--nack-0)

Failed messages are now immediately made available for redelivery instead of
waiting for the subscription's `ackDeadlineSeconds` to expire. This matches the
behaviour of Google's own first-party client libraries.

To keep the 1.x behaviour, set the option explicitly:

```elixir
{BroadwayCloudPubSub.Pull.Producer,
 goth: MyApp.Goth,
 subscription: "...",
 on_failure: :noop}
```

See the [full upgrade guide](docs/upgrade_to_2.0.md) for all details.

## License

Copyright 2019 Michael Crumm \
Copyright 2020 Dashbit

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
