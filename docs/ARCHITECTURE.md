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
2. Allocate the per-peer `outbound` channel (buffered at `BrokerConfig.SubscriberBuffer`, default 256) and the `done` signal channel.
3. Send `brokerEventConnect` to the broker loop. This is what registers the peer.
4. Spawn the writer goroutine.
5. Read `PeerEvent` frames in a `for` loop. A subscribe, unsubscribe, or ping is sent to the broker loop as a `brokerEventPeerEvent`. A publish is fanned out here, on this goroutine, by `deliverPublish` (see [Publisher backpressure](#publisher-backpressure)).
6. On any read error, exit the loop, close `conn`, and send a `brokerEventDisconnect` carrying the `outbound` channel as an identity token.

The identity token in step 6 is the fix for the stale-disconnect race. See [Stale-disconnect handling](#stale-disconnect-handling) below.

### Per-peer writer

`runConnectionWriter` drains `outbound` onto the socket, setting a write deadline of `BrokerConfig.WriteTimeout` (default 30s) before each write. Each frame on `outbound` is already a complete length-prefixed byte slice (encoded once by the broker loop for an ack, or by `deliverPublish` for a fan-out), so the writer just calls `conn.Write` and never marshals. A write that blocks past the deadline closes the connection and exits the writer, so a wedged peer cannot strand its writer goroutine. The writer also exits on either of two other conditions:

- `done` is closed (the broker loop has removed or evicted the peer)
- `broker.shutdown` is closed (the broker is going down)

`outbound` is never closed; the broker loop signals removal through `done` instead, so a publishing reader can send to a subscriber's `outbound` without racing a close. This termination set is what guarantees no writer goroutine outlives its connection.

### Broker loop

`runBrokerLoop` is the only goroutine that touches the `peers` and `subscriptions` maps. Every state transition arrives through the `events` channel as a `brokerEvent` value, and the loop processes one event at a time. Because there is exactly one mutator, the maps need no locks.

Variants:

| Kind | Source | Handler |
|------|--------|---------|
| `brokerEventConnect` | connection reader, first frame | `registerPeer` |
| `brokerEventPeerEvent` | connection reader, subscribe/unsubscribe/ping | `handlePeerEvent` -> `addSubscription` / `removeSubscription` |
| `brokerEventPublish` | connection reader, publish frame | `collectSubscribers` (snapshot returned to the reader) |
| `brokerEventDisconnect` | connection reader, on exit | `removePeerIfCurrent` |

A publish does not deliver on the loop. The loop only runs `collectSubscribers`, which snapshots the handles of every peer currently subscribed to the topic and replies to the publishing reader; the reader does the actual sends (see [Publisher backpressure](#publisher-backpressure)). Keeping the blocking work off the loop is what lets the loop stay non-blocking while still backpressuring a slow subscriber.

After applying a `subscribe` or `unsubscribe`, the loop calls `acknowledge`, which sends an `ack` frame echoing the request's `seq` back onto that peer's `outbound`. The ack send is **non-blocking** so it cannot stall the loop behind a slow peer: if the buffer is full the ack is dropped and logged, and the client's bounded wait eventually fires. Because subscribe and unsubscribe are idempotent, a client whose ack was dropped can retry safely.

### Publisher backpressure

When a connection reader decodes a `publish`, it does not hand a deliverable to the loop. Instead `deliverPublish`:

1. Encodes the `message` frame **once** into a length-prefixed byte slice.
2. Sends a `brokerEventPublish` carrying a buffered `reply` channel and waits for the loop to reply with the topic's subscriber snapshot (`[]peerHandle`).
3. Sends the same byte slice to each subscriber's `outbound` channel, selecting on the subscriber's `done` (skip a peer that disconnected mid-fan-out) and `broker.shutdown`.

Because step 3 runs on the publishing connection's own reader goroutine, a subscriber whose `outbound` buffer is full blocks that reader. The reader then stops calling `ReadFrame`, the publisher's kernel send buffer fills, and the publishing client's next `Publish` write blocks (bounded by its `ctx` deadline). The publisher is throttled to the slowest subscriber's rate, and nothing is silently dropped. The broker loop, meanwhile, only ever did an O(subscribers) snapshot and is free to service other peers. Encoding the frame once rather than per-subscriber also removes the per-recipient marshal cost on a large fan-out.

### Stale-disconnect handling

If a peer with the existing ID `"X"` reconnects:

1. `registerPeer` evicts the old peer (closes its `conn` and its `done` signal).
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
4. Broker's `runConnectionReader` for the publisher decodes the frame and calls `deliverPublish`, which encodes the `message` frame once and sends a `brokerEventPublish` into `events`.
5. `runBrokerLoop` runs `collectSubscribers` and replies with the snapshot of subscriber handles for `"weather/current"`.
6. `deliverPublish` sends the encoded bytes into each subscriber's `outbound` channel, blocking on a full channel (this is the backpressure point) and skipping a subscriber whose `done` has fired.
7. Each subscriber's `runConnectionWriter` receives the bytes from `outbound` and writes them straight to the socket (no re-marshal).
8. Each subscriber's `runClientReader` reads the frame, sees `kind: "message"`, and pushes a `BrokerMessage{Topic, Payload}` to its `inbox`.
9. The application reading from `Inbox(client)` receives the message.

Total goroutines involved for a publish to N subscribers: 1 publisher writer (the caller), 1 reader on the publisher side (which does the fan-out), 1 broker loop, 1 reader and 1 writer per subscriber. The broker loop never blocks on a slow subscriber; the publishing reader does, which is how backpressure reaches the publisher.

## Design notes

The wire types are plain structs with no methods. State for the broker and client lives in plain struct fields. Behaviour is in package-level functions that take the state as their first argument: `Publish(ctx, c, ...)`, `Subscribe(ctx, c, ...)`, `registerPeer(b, ...)`, `deliverPublish(b, ...)`. The only methods are resource lifecycle (`Close`, `Shutdown`, which satisfy the usual `defer x.Close()` shape) and trivial accessors (`ID`, `Address`). Data-oriented, not object-oriented.

One goroutine on the broker touches the peer and subscription maps. No locks, no `sync.Map`. Any new operation becomes a new variant of `brokerEvent` handled in the same loop. Adding a feature does not introduce a new mutator.

`json.RawMessage` carries application payloads verbatim. The broker never re-encodes a publish, never reflects on payload shape, never needs to know an application's schema. A fan-out frame is encoded once and the same bytes go to every subscriber.

The fan-out path backpressures the publisher: a full subscriber buffer blocks the publishing reader rather than dropping the message. The ack path is the one place that still drops on overflow, because an ack is small, idempotent, and retried by the client's bounded wait, and the broker loop must not block on it. Nothing grows without bound.

The stale-disconnect identity check uses the per-connection `outbound` channel as the token: a disconnect event is honoured only if the peer currently registered under that ID still owns the same channel. A reconnect under the same ID installs a fresh channel, so the evicted connection's late disconnect no longer matches and is dropped. No generation counter is needed.

## What is intentionally not here

- No bridges, no broker-to-broker federation.
- No persistence or replay. A publish fans out to the subscribers connected at the time; a slow subscriber backpressures the publisher rather than buffering without bound, and a message is never stored for a subscriber that connects later.
- No reconnection. `Client.Close` is final. A consumer that wants reconnection wraps `ConnectClient` in a retry loop.
- No TLS. Add a `crypto/tls` wrapper around the conn in `StartBroker`/`ConnectClient` if you need it.
- No auth. Anyone who can reach the TCP port can publish or subscribe to any topic.

Each of these is a deliberate omission to keep the surface small. Adding them is a straightforward extension of the same goroutine layout.
