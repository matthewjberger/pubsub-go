# pubsub-go

[![CI](https://github.com/matthewjberger/pubsub-go/actions/workflows/go.yml/badge.svg)](https://github.com/matthewjberger/pubsub-go/actions/workflows/go.yml)

A small topic-based pub/sub broker and client for Go. Clients connect over TCP, subscribe to topics, and publish JSON payloads that the broker fans out to every matching subscriber. There is no queue, no persistence, and no framework. The whole thing is a few hundred lines of Go on top of the standard library.

The wire format is one length-prefixed JSON frame per message (a `uint32` big-endian length followed by that many bytes of JSON), so any language with TCP and JSON can speak it. The Go API is data-oriented: state lives in plain structs, and the operations are package-level functions that take the state as their first argument.

## Prerequisites

- **Go 1.23+** on PATH. Earlier versions probably work but are not tested.
- **[just](https://github.com/casey/just) on PATH** for the convenience recipes. This is optional. Every recipe is a thin `go build` / `go run` / `go test` wrapper, so a plain `go` toolchain is enough.
- Windows uses PowerShell for the `[windows]` recipe variants and Unix uses sh. No other shell setup is required.

## Quickstart

The fastest way to see it work is the one-shot demo. It builds the three binaries and runs broker, publisher, and subscriber in the current shell. Ctrl-C exits cleanly and stops the background processes.

```powershell
just demo
```

To watch each process's log stream in its own terminal, open three shells:

```powershell
# terminal 1
just broker
# terminal 2
just subscriber
# terminal 3
just publisher
```

The same thing without `just`:

```powershell
go run ./cmd/broker
go run ./cmd/subscriber
go run ./cmd/publisher
```

Each binary takes flags (broker address, publish interval, topics). Run it with `--help` for the full list.

## Library usage

The runnable demos under `cmd/` are the most realistic examples:

- [`cmd/broker/main.go`](cmd/broker/main.go) starts a broker and blocks on SIGINT.
- [`cmd/publisher/main.go`](cmd/publisher/main.go) connects and publishes on a ticker.
- [`cmd/subscriber/main.go`](cmd/subscriber/main.go) connects, subscribes, and ranges over the inbox.

Every operation takes a `context.Context`. It bounds the connect, bounds each write, and bounds the wait for a subscribe acknowledgement. The minimal call sequence in a single process:

```go
import (
    "context"
    "fmt"

    "github.com/matthewjberger/pubsub-go/pubsub"
)

ctx := context.Background()

broker, _ := pubsub.StartBroker(pubsub.BrokerConfig{Address: "127.0.0.1:9000"})
defer broker.Shutdown()

client, _ := pubsub.ConnectClient(ctx, pubsub.ClientConfig{
    ID:      "demo",
    Address: broker.Address(),
})
defer client.Close()

pubsub.Subscribe(ctx, client, "weather/current")
pubsub.Publish(ctx, client, "weather/current", map[string]any{"temp_c": 21.4})

message := <-pubsub.Inbox(client)
fmt.Printf("%s -> %s\n", message.Topic, message.Payload)
```

`Subscribe` is synchronous. When it returns nil, the broker has recorded the subscription, so the publish that follows cannot race ahead of it.

Full API reference and patterns (reconnect wrapper, typed topic helpers, multi-topic fan-in): [`docs/USAGE.md`](docs/USAGE.md).

## Design

State for the broker and client lives in plain struct fields. Behaviour lives in package-level functions that take the state as their first argument (`Publish`, `Subscribe`, `Unsubscribe`, `Inbox`, `registerPeer`, `deliverPublish`). The only methods are resource lifecycle (`Close`, `Shutdown`, which satisfy the usual `defer x.Close()` shape) and trivial accessors (`ID`, `Address`). It is data-oriented, not object-oriented.

One goroutine on the broker touches the peer and subscription maps. Reader goroutines funnel events in through a buffered channel, and writer goroutines drain per-peer outbound channels back out. Because there is exactly one mutator, the maps need no locks. Any new operation becomes a new event variant handled in the same loop rather than a new mutator.

Application payloads are `json.RawMessage` end to end. The broker never reflects on the payload shape, never re-encodes a publish, and never needs to know an application's schema. A fan-out frame is encoded once and the same bytes are written to every subscriber, so a publish to N subscribers marshals once, not N times.

Subscribe and unsubscribe are synchronous. The client stamps a sequence number on the request and the broker echoes it back in an acknowledgement frame, so a successful return means the broker has applied the change. Publishes are backpressured, not dropped: the broker fans a publish out on the publishing connection's own reader goroutine, so a subscriber whose per-subscriber buffer (`SubscriberBuffer`, default 256) is full stalls that reader, which stops draining the publisher's socket and throttles the publisher to the slowest subscriber's rate. Nothing is silently discarded.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full goroutine/channel diagram and [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the wire spec.

## Wire format

Every TCP frame is:

```
+---------------------+----------------------------+
| u32 big-endian len  | JSON body of `len` bytes   |
+---------------------+----------------------------+
```

### Client to broker (`PeerEvent`)

```json
{"kind":"connect","id":"weather-pub"}
{"kind":"subscribe","topic":"weather/current","seq":1}
{"kind":"publish","topic":"weather/current","payload":{"temp_c":21.4}}
{"kind":"unsubscribe","topic":"weather/current","seq":2}
{"kind":"ping"}
```

The first frame after dialing must be a `connect`. Anything else closes the connection. `id` is only meaningful on the connect frame; the broker identifies every later frame by the connection it arrived on. `seq` on a subscribe or unsubscribe is the number the broker echoes back in its acknowledgement.

### Broker to client

```json
{"kind":"message","topic":"weather/current","payload":{"temp_c":21.4}}
{"kind":"ack","seq":1}
```

A `message` carries one published payload, byte-for-byte the bytes the publisher sent. An `ack` confirms that the broker applied the subscribe or unsubscribe with the matching `seq`.

Full spec, delivery semantics, and a minimal Python interop client: [`docs/PROTOCOL.md`](docs/PROTOCOL.md).

## What this is not

- Not a queue. Messages are not persisted. A subscriber that connects after a publish does not see the historical message.
- Not durable. There is no on-disk log and no replay; a message lives only long enough to be handed to the subscribers connected when it is published. Delivery to those subscribers is backpressured rather than best-effort: a slow subscriber throttles the publisher instead of having its messages dropped. (A subscriber that disconnects mid-fan-out is skipped, and a stuck subscriber that stops reading is evicted after `WriteTimeout`.)
- No broker-to-broker federation, no bridges.
- No auth, no TLS. Anyone who can reach the TCP port can publish or subscribe to any topic.
- No reconnection. `Client.Close` is final. Wrap `ConnectClient` in a retry loop if you want reconnect-on-disconnect.
- No wildcard topics. Subscribe to the exact string you want.

These omissions are deliberate. They keep the surface small. If you need any of them, this is not the library for your use case, and adding them is a straightforward extension of the same goroutine layout.

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) goroutine/channel layout, broker loop, stale-disconnect handling
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md) wire format, lifecycle, delivery semantics, multi-language interop
- [`docs/USAGE.md`](docs/USAGE.md) API reference and common patterns
- [`docs/LICENSING.md`](docs/LICENSING.md) dual MIT + Apache-2.0 licensing rationale
- [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md) contribution policy and local dev recipes

Tasks are driven through a [`justfile`](justfile). Run `just --list` to see every recipe.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option. See [`docs/LICENSING.md`](docs/LICENSING.md).
