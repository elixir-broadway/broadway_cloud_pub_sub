# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed
- Bump minimum elixir to 1.15
- Modernize github actions

## [1.0.0] - 2026-05-26

### Changed

- Automatically retry 408, 500, 502, 503, 504, 522, 524 errors

## [0.9.1] - 2024-06-21

### Changed

- Forward compatibility with Broadway v1.1

- Handle messages without data

## [0.9.0] - 2023-11-06

### Changed

- Requires Elixir v1.11+.

- Relax `nimble_options` dependency.

- Optional dependency `goth` must be v1.3+.

- **Possible breaking change:** The default token generator requires the `:goth` option to fetch tokens.

  If you had not yet upgraded to Goth v1.3+ please follow the [upgrade guide][goth_upgrade].
  Then wherever you invoke `Broadway.start_link/2`, add the `:goth` option:

  ```elixir
  producer: [
    module:
      {BroadwayCloudPubSub.Producer,
       goth: MyApp.Goth,
       subscription: "projects/<your-project-id>/subscriptions/<your-subscription-id>"}
  ]
  ```

  If you had previously upgraded to Goth v1.3+ then wherever you invoke `Broadway.start_link/2`,
  you may have something like the following:

  ```elixir
  producer: [
    module:
      {BroadwayCloudPubSub.Producer,
       subscription: "projects/<your-project-id>/subscriptions/<your-subscription-id>",
       token_generator: {MyApp, :fetch_token, []}}
  ]
  ```

  ...where `MyApp.fetch_token/0` is similar to the following:

  ```elixir
  defmodule MyApp
    def fetch_token do
      with {:ok, token} <- Goth.fetch(MyApp.Goth) do
        {:ok, token.token}
      end
    end
  end
  ```

  You can remove your custom token generator function and replace your producer config with this:


  ```elixir
  producer: [
    module:
      {BroadwayCloudPubSub.Producer,
       goth: MyApp.Goth,
       subscription: "projects/<your-project-id>/subscriptions/<your-subscription-id>"}
  ]
  ```

### Added

- The `:goth` option specifies the [Goth][goth] server for
  the default token generator.

### Removed

- The `:scope` option has been removed. You may set custom scopes on your own [Goth][goth] server
  via the `:source` option.

  For example, if you want to use the scope `"https://www.googleapis.com/auth/pubsub"`, then
  wherever you start Goth add the `:scopes` option to your authentication source:

  ```elixir
    # The `:metadata` source type retrieves credentials from Google metadata servers.
    # Refer to the Goth documentation for more source options.
    source = {:metadata, scopes: ["https://www.googleapis.com/auth/pubsub"]}

    children = [
      {Goth, name: MyApp.Goth, source: source}
    ]
  ```

## [0.8.0] - 2022-10-26

This version moves Cloud PubSub from Tesla to Finch, so read the notes below and upgrade with care.

### Added

- Use `:finch` as the HTTP client and provide a `:finch` producer option for a user-defined HTTP pool

- Make HTTP requests in a separate process for cleaner shutdown

- Support multiple topologies from the same Brodway module

- Add telemetry events around HTTP requests

- Add `:deliveryAttempt` field to metadata

### Removed

- The `:pool_size` option has been removed. Define your own [Finch][finch]
  pool and use the `:finch` option instead.

  For example, if your `:pool_size` was 10, then add Finch to your application
  supervision tree (usually located in `lib/my_app/application.ex`):

  ```elixir
  children =
    [
      {Finch, name: MyFinch, pools: %{:default => [size: 10]}}
    ]
  ```

  ...and wherever you invoke `Broadway.start_link/2`, replace this:

  ```elixir
  producer: [
    module:
      {BroadwayCloudPubSub.Producer,
       subscription: "projects/<your-project-id>/subscriptions/<your-subscription-id>",
       pool_size: 10}
  ]
  ```

  ...with this:

  ```elixir
  producer: [
    module:
      {BroadwayCloudPubSub.Producer,
       subscription: "projects/<your-project-id>/subscriptions/<your-subscription-id>",
       finch: MyFinch}
  ]
  ```

### Changed

- **Possible breaking change:** due to the fact that we've switched from Tesla to Finch,
  it's not longer sufficient to replace `:base_url` in `:google_api_pub_sub` config.

  If your app is using the emulator, use following settings in the `config/dev.exs`:

  ```elixir
  producer: [
    module:
      {BroadwayCloudPubSub.Producer,
       subscription: "projects/<your-project-id>/subscriptions/<your-subscription-id>",
       base_url: System.get_env("PUBSUB_EMULATOR_HOST")}
  ]
  ```

## [0.7.1] - 2022-05-09

### Added

- Add `:receive_timeout` option

## [0.7.0] - 2021-08-30

### Changed

- Require Broadway 1.0

## [0.6.3] - 2021-07-19

### Changed

- Remove sensitive details from error log messages

### Added

- A new `:middleware` option to pass a list of custom Tesla middleware
- Failures to ack are now automatically retried. Retries can be customised via the new `:retry` option

## [0.6.2] - 2021-02-24

### Changed

- Fixed a bug causing malformed acknowledgement requests (#54)

## [0.6.1] - 2021-02-23

### Changed

- Decreased maximum number of ackIds per request (#49)

## [0.6.0] - 2020-02-18

### Added

- Support for passing a tuple for the `scope` option

### Changed

- Require [Broadway v0.6.0](https://hexdocs.pm/broadway/0.6.0)

## [0.5.0] - 2019-11-06

### Added

- Client options for connection pools (#37)

- Support for configuring acknowledgement behavior (#36)

### Changed

- Move acknowledger behaviour from `GoogleApiClient` into `ClientAcknowledger` (#39)

## [0.4.0] - 2019-08-19

### Changed

- Move to Plataformatec GitHub organization and become an official Broadway connector

- Rename behaviour `RestClient` to `Client` (#23)

- Use hackney as the default adapter (#20)

- Require Broadway 0.4.x and (optionally) Goth 1.x (#26)

- Replace `:token_module` option with `:token_generator` (#29)

- Hide `handle_receive_messages` function that was accidentally made public

## [0.3.0] - 2019-05-08

### Changed

- **BREAKING:** The PubsubMessage struct now gets unpacked into the `%Broadway.Message{}` received in your pipeline.  If you were using `message.data.data` before, you can now use `message.data`. Additional properties from the PubsubMessage can be found in the message metadata, for instance: `message.metadata.attributes` or `message.metadata.messageId`.
- Requires `:broadway ~> 0.3.0`

## [0.1.3] - 2019-05-06

### Changed

- Fixed `BroadwayCloudPubSub.GoogleApiClient` attempting to send an empty acknowledge request.

## [0.1.2] - 2019-04-11

### Changed

- MIX_ENV for publishing releases to Hex.

## [0.1.1] - 2019-04-11

### Added

- This `CHANGELOG` file to hopefully serve as an evolving example of a
  standardized open source project `CHANGELOG`.

### Changed

- Fixed CircleCI build for publishing docs to Hex.

## [0.1.0] - 2019-04-10

### Added

- `BroadwayCloudPubSub.Producer` - A GenStage producer that continuously receives messages from
    a Pub/Sub subscription acknowledges them after being successfully processed.
- `BroadwayCloudPubSub.RestClient` - A generic behaviour to implement Pub/Sub clients using the [REST API](https://cloud.google.com/pubsub/docs/reference/rest/).
- `BroadwayCloudPubSub.GoogleApiClient` - Default REST client used by `BroadwayCloudPubSub.Producer`.
- `BroadwayCloudPubSub.Token` - A generic behaviour to implement token authentication for Pub/Sub clients.
- `BroadwayCloudPubSub.GothToken` - Default token provider used by `BroadwayCloudPubSub.Producer`.

[1.0.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.9.1...v1.0.0
[0.9.1]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.6.2...v0.7.0
[0.6.3]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.1.3...v0.3.0
[0.1.3]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/dashbitco/broadway_cloud_pub_sub/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/dashbitco/broadway_cloud_pub_sub/releases/tag/v0.1.0


[finch]: https://hexdocs.pm/finch
[goth]: https://hexdocs.pm/goth
[goth_upgrade]: https://hexdocs.pm/goth/upgrade_guide.html
