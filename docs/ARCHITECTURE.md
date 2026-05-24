# Architecture

This document walks the runtime structure of `pubsub-go`. The broker and client are designed around a small handful of goroutines and channels and one wire protocol; understanding those is enough to predict any behaviour.

All file paths in this doc are relative to the repo root.

## Layers

```
Application Layer:  cmd/broker, cmd/publisher, cmd/subscriber
                            |
Client Layer:       pubsub/client.go            (Client, Publish, Subscribe, Inbox)
                            |
Broker Layer:       pubsub/broker.go            (Broker, accept loop, broker loop)
                            |
Protocol Layer:     pubsub/protocol.go          (PeerEvent, BrokerMessage)
                    pubsub/frame.go             (WriteFrame, ReadFrame)
                            |
Foundation:         net, encoding/json, encoding/binary, bufio
```

There is no foundation runtime in the project itself; every layer is built directly on the Go standard library.

## Foundation Layer

**`pubsub/protocol.go`** declares the wire types. `PeerEvent` (client to broker) and `brokerFrame` (broker to client) are both flat tagged unions: a `kind` string selects which other fields are meaningful. `PeerEvent` covers `connect`, `publish`, `subscribe`, `unsubscribe`, and `ping`; `brokerFrame` covers `message` (a delivered publish) and `ack` (a subscribe/unsubscribe confirmation echoing the request's `seq`). `BrokerMessage` is the decoded `{topic, payload}` value a consumer reads off the inbox, derived from a `message`-kind `brokerFrame`. Payloads are held as `json.RawMessage` so the broker never has to know an application's schema.

**`pubsub/frame.go`** declares the framing: one frame is a `uint32` big-endian length followed by exactly that many bytes of JSON. `WriteFrame(io.Writer, any)` marshals a value and writes one frame in a single `Write` call. `ReadFrame(*bufio.Reader, any)` reads the header, the body, then `json.Unmarshal`s into the target. Both functions reject frames longer than `MaxFrameSize` (16 MiB).

These two files are the entire protocol surface. Any language with TCP, JSON, and a big-endian `uint32` can speak it.

## Broker Layer

**`pubsub/broker.go`** declares `Broker`, started by `StartBroker(BrokerConfig)`. A running broker has three kinds of goroutine:

```
                    +---------------+
                    | runBrokerLoop |  <-- single mutator of peers + subscriptions
                    +---------------+
                          ^
                          | brokerEvent
                          |
   +----------------+     |     +----------------------+
   | runAcceptLoop  |---->|<----| runConnectionReader  |   (one per peer)
   +----------------+     |     +----------------------+
                          |              |
                          |              | BrokerMessage
                          |              v
                          |     +----------------------+
                          |     | runConnectionWriter  |   (one per peer)
                          |     +----------------------+
                          |
                          | shutdown
                  +-------+--------+
                  | Broker.Shutdown |
                  +----------------+
```

### Accept loop

`runAcceptLoop` calls `listener.Accept` in a hot loop. Each accepted `net.Conn` gets a dedicated reader goroutine. Exits on `net.ErrClosed` (clean shutdown) or any other error after one log line.

### Per-peer reader

`runConnectionReader` is responsible for one TCP connection's lifetime:

1. Read the first frame. It must be a `PeerEventConnect` with a non-empty ID; if not, close and return.
2. Allocate the per-peer `outbound` channel (buffered at 64) and the `connClosed` signal channel.
3. Send `brokerEventConnect` to the broker loop. This is what registers the peer.
4. Spawn the writer goroutine.
5. Read `PeerEvent` frames in a labelled `for readLoop` and send each to the broker loop as a `brokerEventPeerEvent`.
6. On any read error, exit the loop, close `connClosed`, close `conn`, and send a `brokerEventDisconnect` carrying the `outbound` channel as an identity token.

The identity token in step 6 is the fix for the stale-disconnect race. See [Stale-disconnect handling](#stale-disconnect-handling) below.

### Per-peer writer

`runConnectionWriter` drains `outbound` onto the socket via `WriteFrame`, setting a write deadline of `BrokerConfig.WriteTimeout` (default 30s) before each write. A write that blocks past the deadline closes the connection and exits the writer, so a wedged peer cannot strand its writer goroutine. The writer also exits on any of three conditions:

- `outbound` is closed (the broker loop has dropped the peer)
- `connClosed` is closed (the reader has detected a dropped connection)
- `broker.shutdown` is closed (the broker is going down)

This termination set is what guarantees no writer goroutine outlives its connection.

### Broker loop

`runBrokerLoop` is the only goroutine that touches the `peers` and `subscriptions` maps. Every state transition arrives through the `events` channel as a `brokerEvent` value, and the loop processes one event at a time. Because there is exactly one mutator, the maps need no locks.

Variants:

| Kind | Source | Handler |
|------|--------|---------|
| `brokerEventConnect` | connection reader, first frame | `registerPeer` |
| `brokerEventPeerEvent` | connection reader, subsequent frames | `handlePeerEvent` -> `publishToSubscribers` / `addSubscription` / `removeSubscription` |
| `brokerEventDisconnect` | connection reader, on exit | `removePeerIfCurrent` |

`publishToSubscribers` does **best-effort** delivery: if a subscriber's outbound buffer is full it logs and drops the message. A slow subscriber cannot block the broker loop or any other subscriber.

After applying a `subscribe` or `unsubscribe`, the loop calls `acknowledge`, which sends an `ack` frame echoing the request's `seq` back onto that peer's `outbound`. The ack rides the same best-effort path as a delivery: if the buffer is full it is dropped and logged, and the client's bounded wait eventually fires. Because subscribe and unsubscribe are idempotent, a client whose ack was dropped can retry safely.

### Stale-disconnect handling

If a peer with the existing ID `"X"` reconnects:

1. `registerPeer` evicts the old peer (closes its `conn` and `outbound`).
2. The old reader's `ReadFrame` fails with `net.ErrClosed`, falls through to its exit path, and sends `brokerEventDisconnect{peerID: "X", outbound: oldOutbound}`.
3. By then the broker has already installed the new peer at `peers["X"]` with `newOutbound`.
4. `removePeerIfCurrent` sees `peers["X"].outbound != oldOutbound` and ignores the stale event. The new peer is unaffected.

Carrying `outbound` (or any per-connection identity) on the disconnect event is enough; no monotonic generation counter is needed.

## Client Layer

**`pubsub/client.go`** declares `Client`, dialed by `ConnectClient(ctx, ClientConfig)`. The dial uses `net.Dialer.DialContext`, so `ctx` bounds the connect. A running client has one goroutine:

```
        +-------------------+
        |  runClientReader  |  <-- reads brokerFrame; message -> inbox, ack -> waiter
        +-------------------+
                  |
                  v
            inbox channel
                  |
                  v
            consumer code
```

`runClientReader` decodes each `brokerFrame`. A `message` frame becomes a `BrokerMessage` pushed onto `inbox`; an `ack` frame wakes the matching waiter (see below). Acknowledgements never reach the inbox.

Outbound calls (`Publish`, `Subscribe`, `Unsubscribe`, `Ping`) all funnel through `writeEvent`, which takes a mutex around the socket so concurrent callers cannot interleave frames on the wire. If `ctx` carries a deadline, `writeEvent` sets it on the socket for the duration of the write. The mutex check-and-write also covers the `closed` channel so writes against a closed client return `ErrClosed` (a package sentinel, distinct from `net.ErrClosed`).

`Subscribe` and `Unsubscribe` are synchronous. `awaitAck` stamps the `PeerEvent` with the next sequence number from an `atomic.Uint64`, registers a waiter channel in the `pending` map under that sequence, writes the frame, then blocks until one of three things happens: the reader delivers the matching `ack` and closes the waiter, `ctx` is cancelled, or the client closes. The map is guarded by `pendingMu`; the reader deletes and closes a waiter when the ack arrives, and the cancel/close paths delete their own entry so the map never leaks. Matching by sequence is what lets concurrent subscribes coexist without confusing each other's replies.

`Client.Close` is the single shutdown path. It uses `sync.Once` to make repeat calls a no-op:

1. Close `closed` (cancels any pending writes, releases any `awaitAck` waiter with `ErrClosed`, and unblocks an inbox-send in the reader).
2. Close `conn` (the reader's `ReadFrame` returns).
3. Wait on `readDone` (the reader has exited).
4. Close `inbox` (consumers ranging over `Inbox(client)` see EOF).

In-flight messages already in `inbox` are still delivered to consumers before they see the channel close.

## Application Layer

The three binaries in `cmd/` are thin wrappers. `cmd/broker/main.go` calls `pubsub.StartBroker` and blocks on a signal. `cmd/publisher/main.go` connects, publishes on a ticker, and exits on signal. `cmd/subscriber/main.go` connects, subscribes to one or more topics, and prints every received message. The publisher/subscriber pair exists to exercise the end-to-end protocol from the command line.

## Data flow: a single publish

1. Publisher calls `pubsub.Publish(ctx, client, "weather/current", weather)`.
2. `Publish` marshals `weather` to JSON, calls `PublishRaw`, which calls `writeEvent`.
3. `writeEvent` takes `writeMu`, writes one length-prefixed frame containing `{"kind":"publish","topic":"weather/current","payload":{...}}`.
4. Broker's `runConnectionReader` for the publisher decodes the frame and sends `brokerEventPeerEvent` into `events`.
5. `runBrokerLoop` dispatches to `handlePeerEvent` -> `publishToSubscribers`.
6. For each subscriber of `"weather/current"`, the loop sends a `brokerFrame{Kind: "message", Topic, Payload}` into that subscriber's `outbound` channel (non-blocking; drops on overflow).
7. Each subscriber's `runConnectionWriter` receives from `outbound` and writes one length-prefixed frame back over TCP.
8. Each subscriber's `runClientReader` reads the frame, sees `kind: "message"`, and pushes a `BrokerMessage{Topic, Payload}` to its `inbox`.
9. The application reading from `Inbox(client)` receives the message.

Total goroutines involved for a publish to N subscribers: 1 publisher writer (the caller), 1 reader on the publisher side, 1 broker loop, 1 reader and 1 writer per subscriber. None of them block any other.

## Design notes

The wire types are plain structs with no methods. State for the broker and client lives in plain struct fields. Behaviour is in package-level functions that take the state as their first argument: `Publish(ctx, c, ...)`, `Subscribe(ctx, c, ...)`, `registerPeer(b, ...)`, `publishToSubscribers(b, ...)`. The only methods are resource lifecycle (`Close`, `Shutdown`, which satisfy the usual `defer x.Close()` shape) and trivial accessors (`ID`, `Address`). Data-oriented, not object-oriented.

One goroutine on the broker touches the peer and subscription maps. No locks, no `sync.Map`. Any new operation becomes a new variant of `brokerEvent` handled in the same loop. Adding a feature does not introduce a new mutator.

`json.RawMessage` carries application payloads verbatim. The broker never re-encodes a publish, never reflects on payload shape, never needs to know an application's schema.

Every channel is buffered with a small capacity and either drops on overflow (the broker's fan-out path and the ack path) or backpressures the caller (client writes). Nothing grows without bound.

The stale-disconnect identity check uses the per-connection `outbound` channel as the token: a disconnect event is honoured only if the peer currently registered under that ID still owns the same channel. A reconnect under the same ID installs a fresh channel, so the evicted connection's late disconnect no longer matches and is dropped. No generation counter is needed.

## What is intentionally not here

- No bridges, no broker-to-broker federation.
- No deferred or rate-limited publish queue. A publish fans out immediately or is dropped for a full subscriber.
- No reconnection. `Client.Close` is final. A consumer that wants reconnection wraps `ConnectClient` in a retry loop.
- No TLS. Add a `crypto/tls` wrapper around the conn in `StartBroker`/`ConnectClient` if you need it.
- No auth. Anyone who can reach the TCP port can publish or subscribe to any topic.

Each of these is a deliberate omission to keep the surface small. Adding them is a straightforward extension of the same goroutine layout.
