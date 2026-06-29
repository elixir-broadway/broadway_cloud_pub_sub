# Upgrading to 2.0

2.0 introduces a new default producer built on the gRPC StreamingPull API. The
previous HTTP pull producer is still fully supported, but has moved to its own
sub-namespace as a fallback for environments where gRPC is unavailable.

## Overview of breaking changes

| # | What changed | Migration action |
|---|---|---|
| [1](#1-new-default-producer) | `BroadwayCloudPubSub.Producer` is now the **streaming** producer | Switch to streaming, or rename to `Pull.Producer` to keep pull |
| [2](#2-broadwaycloudpubsubpullclient-renamed) | `BroadwayCloudPubSub.PullClient` → `BroadwayCloudPubSub.Pull.FinchClient` | Rename if referenced directly |
| [3](#3-broadwaycloudpubsubclient-behaviour-renamed) | `BroadwayCloudPubSub.Client` behaviour → `BroadwayCloudPubSub.Pull.Client` | Rename if you implemented a custom pull client |
| [4](#4-on_failure-default-changed-noop--nack-0) | `on_failure` default: `:noop` → `{:nack, 0}` | Set `on_failure: :noop` explicitly to keep old behaviour |

---

## Should you switch to streaming?

The short answer from Google's [Pub/Sub documentation][gcp-pull] is: yes, in
almost all cases.

**StreamingPull** ([reference][gcp-streamingpull]) is what Google's own
first-party client libraries use "where possible" because it minimises latency
and maximises throughput. It uses a persistent bidirectional gRPC connection:
the server pushes messages as they become available, applies flow control via
outstanding-message and outstanding-byte limits, and the client library extends
ack deadlines automatically.

**Unary Pull** ([reference][gcp-pull-api]) is a traditional request/response
RPC. Google notes that to get high throughput with the Pull API you would need
to maintain many simultaneous outstanding requests, which is "error-prone and
hard to maintain", and recommends StreamingPull instead. Google lists only
these cases where unary Pull is the right choice:

- You need strict control over the number of messages the subscriber processes
  per request.
- You need fine-grained control over client memory, CPU, or network usage.
- Your subscriber is a proxy between Pub/Sub and another service that operates
  in a pull-oriented way.
- gRPC is unavailable or undesired in your environment (for example, an
  HTTP-only network policy).

If none of those apply, switch to the streaming producer.

[gcp-pull]: https://cloud.google.com/pubsub/docs/pull
[gcp-streamingpull]: https://cloud.google.com/pubsub/docs/pull#streamingpull_api
[gcp-pull-api]: https://cloud.google.com/pubsub/docs/pull#pull_api

---

## 1. New default producer

In 2.0, `BroadwayCloudPubSub.Producer` is a brand-new producer that uses the
gRPC StreamingPull API. Instead of polling Pub/Sub over HTTP, it opens a
persistent bidirectional gRPC stream and receives messages as the server pushes
them. This gives lower latency, higher throughput, and removes the need to tune
`ackDeadlineSeconds`. Leases are extended automatically.

You have two migration paths:

### Path A: switch to the new streaming producer (recommended)

Add the gRPC dependencies to `mix.exs`. You must pick one HTTP/2 adapter for
the gRPC connection: either `:gun`, or `:mint` together with `:castore`:

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

Then update your pipeline config.

```elixir
# 1.x
producer: [
  module: {BroadwayCloudPubSub.Producer,
    goth: MyApp.Goth,
    subscription: "projects/my-project/subscriptions/my-sub",
    max_number_of_messages: 100,
    receive_interval: 500}
]

# 2.0, streaming producer
producer: [
  module: {BroadwayCloudPubSub.Producer,
    goth: MyApp.Goth,
    subscription: "projects/my-project/subscriptions/my-sub",
    max_outstanding_messages: 1000}
]
```

The tables below map every 1.x pull option to its 2.0 streaming equivalent.

#### Options that work unchanged

These options have the same name and semantics in the streaming producer:

| Option | Notes |
|---|---|
| `:subscription` | Required. Same format. |
| `:goth` | Same. |
| `:token_generator` | Same MFA tuple interface. |
| `:on_success` | Same values (`:ack`, `:noop`, `{:nack, seconds}`). |
| `:on_failure` | Same values. Default changed to `{:nack, 0}` (see [breaking change #4](#4-on_failure-default-changed-noop--nack-0)). |

#### Options that have a replacement

| 1.x pull option | 2.0 streaming replacement | Notes |
|---|---|---|
| `:max_number_of_messages` | `:max_outstanding_messages` | Controls how many unacknowledged messages the server pushes at once, across the whole stream rather than per-request. |
| `:base_url` | `:grpc_endpoint` | Override the service endpoint. The format differs: `:base_url` takes an HTTP URL (`"https://pubsub.googleapis.com"`), while `:grpc_endpoint` takes a bare `host:port` string (`"localhost:8085"`). `:grpc_endpoint` pairs with `:use_ssl` (boolean, default `true`) to control TLS. |
| `:client` | `:grpc_client` | Plug-in a custom client implementation. Now accepts `Module` or `{Module, opts}`. See `BroadwayCloudPubSub.Streaming.Client`. |

#### Options with no streaming equivalent

The streaming producer manages its own connection lifecycle and flow control, so
these pull options have no direct replacement:

| 1.x pull option | Why it does not apply |
|---|---|
| `:receive_interval` | The stream is persistent; the producer does not poll on a timer. |
| `:receive_timeout` | Timeouts are handled at the gRPC transport level. Use `:backoff_*` options to control reconnection. |
| `:finch` | The streaming producer uses gRPC over Gun or Mint, not Finch. |

The streaming producer also exposes many options that have no pull counterpart,
covering message ordering, flow control tuning, ack batching, reconnection
backoff, graceful shutdown, and more. See `BroadwayCloudPubSub.Producer` for
the full option reference.

### Path B: keep the HTTP pull producer

If gRPC is not available in your environment, you want to continue using the pull producer, or want to do progresive rollout supporting both, simply rename the module:

```elixir
# 1.x
producer: [
  module: {BroadwayCloudPubSub.Producer,
    goth: MyApp.Goth,
    subscription: "projects/my-project/subscriptions/my-sub"}
]

# 2.0, pull producer
producer: [
  module: {BroadwayCloudPubSub.Pull.Producer,
    goth: MyApp.Goth,
    subscription: "projects/my-project/subscriptions/my-sub"}
]
```

All existing options (`:goth`, `:subscription`, `:token_generator`, `:base_url`,
`:max_number_of_messages`, `:receive_interval`, `:on_success`, `:on_failure`,
`:client`) are unchanged.

The `grpc`, `protobuf`, `gun`, `mint` and `castore` dependencies are **not** required when using only
`BroadwayCloudPubSub.Pull.Producer`.

---

## 2. `BroadwayCloudPubSub.PullClient` renamed

`BroadwayCloudPubSub.PullClient` is now `BroadwayCloudPubSub.Pull.FinchClient`.

This only affects you if you reference it directly, for example when overriding
the `:client` option or in tests:

```elixir
# 1.x
client: BroadwayCloudPubSub.PullClient

# 2.0
client: BroadwayCloudPubSub.Pull.FinchClient
```

---

## 3. `BroadwayCloudPubSub.Client` behaviour renamed

`BroadwayCloudPubSub.Client` is now `BroadwayCloudPubSub.Pull.Client`.

This only affects you if you implemented a custom HTTP pull client:

```elixir
# 1.x
defmodule MyApp.CustomPullClient do
  @behaviour BroadwayCloudPubSub.Client
  ...
end

# 2.0
defmodule MyApp.CustomPullClient do
  @behaviour BroadwayCloudPubSub.Pull.Client
  ...
end
```

The callback signatures are unchanged.

---

## 4. `on_failure` default changed: `:noop` → `{:nack, 0}`

In 1.x, failed messages were left to expire and be redelivered after the
subscription's `ackDeadlineSeconds`. In 2.0 the default is `{:nack, 0}`, making
them immediately available for redelivery, matching the behaviour of the official
Google Cloud Pub/Sub client libraries.

**If you relied on the old default**, add `on_failure: :noop` explicitly:

```elixir
# Pull producer
{BroadwayCloudPubSub.Pull.Producer,
 goth: MyApp.Goth,
 subscription: "projects/my-project/subscriptions/my-sub",
 on_failure: :noop}

# Streaming producer
{BroadwayCloudPubSub.Producer,
 goth: MyApp.Goth,
 subscription: "projects/my-project/subscriptions/my-sub",
 on_failure: :noop}
```

For most applications the new default is the right behaviour. A failed message
is retried immediately rather than holding up the subscription until its deadline
expires.

---

## Quick-reference: all renamed modules

| 1.x name | 2.0 name |
|---|---|
| `BroadwayCloudPubSub.Producer` | `BroadwayCloudPubSub.Pull.Producer` (pull, fallback) |
| *(new in 2.0)* | `BroadwayCloudPubSub.Producer` (streaming, recommended) |
| `BroadwayCloudPubSub.PullClient` | `BroadwayCloudPubSub.Pull.FinchClient` |
| `BroadwayCloudPubSub.Client` | `BroadwayCloudPubSub.Pull.Client` |
| `BroadwayCloudPubSub.Options` | BroadwayCloudPubSub.Pull.Options (internal) |
| `BroadwayCloudPubSub.Acknowledger` | BroadwayCloudPubSub.Pull.Acknowledger (internal) |
