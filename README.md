# pubsub-go

[![CI](https://github.com/matthewjberger/pubsub-go/actions/workflows/go.yml/badge.svg)](https://github.com/matthewjberger/pubsub-go/actions/workflows/go.yml)

A small topic-based pub/sub broker and client for Go. Clients connect over TCP, subscribe to topics, and publish JSON payloads that the broker fans out to every matching subscriber. Wire format is one length-prefixed JSON frame per message (`[u32 big-endian length][JSON body]`), so any language with TCP and JSON can speak the protocol.

## What this is not

- Not a queue. Messages aren't persisted; subscribers that connect after a publish do not see the historical message.
- Not at-least-once. Delivery is best-effort with a small per-subscriber buffer; a slow subscriber's messages are dropped silently (and logged broker-side).
- No bridges, no broker-to-broker federation.
- No auth, no TLS. Anyone who can reach the TCP port can publish or subscribe to any topic.
- No reconnection. `Client.Close` is final; wrap `ConnectClient` in a retry loop if you need reconnect-on-disconnect.
- No wildcard topics. Subscribe to the exact string you want to receive.

These omissions are intentional. If you need them, this is not the library for your use case.

## Prerequisites

- **Go 1.23+** on PATH. Earlier versions probably work but aren't tested.
- **[just](https://github.com/casey/just) on PATH** for the convenience recipes. Optional; every recipe is a thin `go build` / `go run` / `go test` wrapper, so a plain `go` toolchain is enough if you want to skip `just`.
- Windows uses PowerShell for the `[windows]` recipe variants; Unix uses sh. No other shell setup is required.

## Quickstart

The fastest way to see it work is the one-shot demo, which builds the three binaries and runs broker + publisher + subscriber in the current shell. Ctrl-C exits cleanly and stops the background processes.

```powershell
just demo
```

If you'd rather see each process's log stream in its own terminal, open three shells and run:

```powershell
# terminal 1
just broker
# terminal 2
just subscriber
# terminal 3
just publisher
```

Equivalent without `just`:

```powershell
go run ./cmd/broker
go run ./cmd/subscriber
go run ./cmd/publisher
```

Each binary accepts flags; see `--help` for the full list (broker address, publish interval, topics).

## Library usage

The runnable demos under `cmd/` are the most realistic examples:

- [`cmd/broker/main.go`](cmd/broker/main.go) — start a broker, block on SIGINT.
- [`cmd/publisher/main.go`](cmd/publisher/main.go) — connect, publish on a ticker.
- [`cmd/subscriber/main.go`](cmd/subscriber/main.go) — connect, subscribe, range over the inbox.

The minimal call sequence in a single process:

```go
import "github.com/matthewjberger/pubsub-go/pubsub"

broker, _ := pubsub.StartBroker(pubsub.BrokerConfig{Address: "127.0.0.1:9000"})
defer broker.Shutdown()

client, _ := pubsub.ConnectClient(pubsub.ClientConfig{
    ID:      "demo",
    Address: broker.Address(),
})
defer client.Close()

pubsub.Subscribe(client, "weather/current")
pubsub.Publish(client, "weather/current", map[string]any{"temp_c": 21.4})

message := <-pubsub.Inbox(client)
fmt.Printf("%s -> %s\n", message.Topic, message.Payload)
```

Full API reference and patterns (reconnect wrapper, typed topic helpers, multi-topic fan-in): [`docs/USAGE.md`](docs/USAGE.md).

## Design

- **Wire types** in [`pubsub/protocol.go`](pubsub/protocol.go) are plain data structs. `PeerEvent` is a flat tagged union (`kind: "connect" | "publish" | "subscribe" | "unsubscribe" | "ping"`) sent client to broker. `BrokerMessage` is `{topic, payload}` sent broker to subscriber. Both serialize as JSON with no methods on the wire types.
- **State** for the broker and client lives in plain struct fields. **Behaviour** is in package-level functions (`Publish`, `Subscribe`, `Unsubscribe`, `Inbox`, `registerPeer`, `publishToSubscribers`). Methods exist only for Go-standard lifecycle (`Close`, `Shutdown`) and trivial accessors (`ID`, `Address`). Data-oriented, not object-oriented; same style as [`wgpu-example-go`](https://github.com/matthewjberger/wgpu-example-go) and [`freecs-go`](https://github.com/matthewjberger/freecs-go).
- **Single mutator on the broker.** One goroutine touches the peer and subscription maps. Reader goroutines funnel events in through a buffered channel; writer goroutines drain per-peer outbound channels back out. No locks on broker state.
- **Application payloads are `json.RawMessage`.** The broker never has to know an application's schema and never re-encodes a publish.

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
{"kind":"subscribe","id":"weather-sub","topic":"weather/current"}
{"kind":"publish","id":"weather-pub","topic":"weather/current","payload":{"temp_c":21.4}}
{"kind":"unsubscribe","id":"weather-sub","topic":"weather/current"}
{"kind":"ping"}
```

The first frame after `net.Dial` must be a `connect`; anything else closes the connection.

### Broker to subscriber (`BrokerMessage`)

```json
{"topic":"weather/current","payload":{"temp_c":21.4}}
```

`payload` is the exact bytes the publisher sent; the broker never re-encodes.

Full spec, delivery semantics, and a minimal Python interop client: [`docs/PROTOCOL.md`](docs/PROTOCOL.md).

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) goroutine/channel layout, broker loop, stale-disconnect handling
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md) wire format, lifecycle, delivery semantics, multi-language interop
- [`docs/USAGE.md`](docs/USAGE.md) API reference and common patterns
- [`docs/LICENSING.md`](docs/LICENSING.md) dual MIT + Apache-2.0 licensing rationale
- [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md) contribution policy and local dev recipes

Tasks are driven through a [`justfile`](justfile); run `just --list` to see every recipe.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option. See [`docs/LICENSING.md`](docs/LICENSING.md).
