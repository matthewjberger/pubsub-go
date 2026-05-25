# Wire Protocol

`pubsub-go` speaks one protocol over TCP: length-prefixed JSON frames. This document specifies the wire format precisely enough that an implementation in any language can interoperate with both the broker and clients shipped here.

## Framing

Every TCP frame is:

```
+---------------------------------+--------------------------------------+
| uint32 length, big-endian (4B)  | UTF-8 JSON body, exactly `length` B  |
+---------------------------------+--------------------------------------+
```

- `length` is unsigned, big-endian, 4 bytes on the wire.
- `length` does not include itself: a 12-byte body is preceded by `0x00 0x00 0x00 0x0C`.
- `length == 0` is illegal; the smallest legitimate JSON body is 2 bytes (`{}`).
- Implementations should reject any frame larger than 16 MiB (`MaxFrameSize`) to bound memory use.
- The body must be valid JSON. The receiver is free to error out on malformed JSON.
- There is no terminator after the body; the next frame's length immediately follows.

## Connection lifecycle

1. **Dial.** The client opens a TCP connection to the broker.
2. **Handshake.** The client's first frame must be a `PeerEvent` with `kind = "connect"` and a non-empty `id`. Anything else makes the broker close the connection.
3. **Operations.** The client sends any sequence of `PeerEvent` frames: `publish`, `subscribe`, `unsubscribe`, `ping`. The broker sends frames back: a `message` frame for every published message that matches a topic the client subscribed to, and an `ack` frame for every `subscribe` and `unsubscribe`.
4. **Close.** Either side closes the TCP connection. The broker drops the peer from its subscription tables on EOF; the client drains any buffered inbound messages and closes its inbox channel.

The protocol is duplex and asynchronous. Publishes get no `ack` frame, but they are not unbounded: the broker applies TCP-level backpressure to a publisher whose subscribers cannot keep up (see [Delivery semantics](#delivery-semantics)). Subscribes and unsubscribes carry a `seq` that the broker echoes back in an `ack`, which is the only request/response correlation in the protocol; a client uses it to make `subscribe` synchronous so a publish cannot race ahead of a subscription.

## Client to broker: `PeerEvent`

`PeerEvent` is a flat tagged union. `kind` selects which other fields are meaningful; fields that do not apply to a `kind` are omitted from the JSON.

```json
{
  "kind":      "connect" | "publish" | "subscribe" | "unsubscribe" | "ping",
  "id":        string (optional),
  "topic":     string (optional),
  "payload":   any JSON value (optional),
  "seq":       unsigned integer (optional)
}
```

`id` is only meaningful on a `connect`. The broker identifies every later frame by the connection it arrived on, not by a field in the frame, so a publish or subscribe does not need to repeat it. `seq` is only meaningful on `subscribe` and `unsubscribe`.

### `connect`

Registers the sender as a peer on the broker. Must be the first frame after dial.

```json
{"kind": "connect", "id": "weather-publisher"}
```

If a peer with the same `id` is already connected, the broker closes the previous connection and accepts the new one (most-recent-wins).

### `publish`

Asks the broker to fan `payload` out to every subscriber of `topic`. The broker does not re-encode `payload`; subscribers receive the exact bytes the publisher sent.

```json
{
  "kind":    "publish",
  "topic":   "weather/current",
  "payload": {"temp_c": 21.4, "humidity": 65}
}
```

`payload` can be any JSON value (object, array, string, number, boolean, null). The broker sends no acknowledgement for a publish. It does, however, stop reading a publisher's connection while any of that publisher's target subscribers is saturated, so a publisher that outruns its subscribers will find its own writes blocking (see [Delivery semantics](#delivery-semantics)).

### `subscribe`

Subscribes the sender to `topic`. Subsequent publishes to that topic are delivered to this peer. Subscribing to the same topic twice is a no-op. Set `seq` to a value the broker will echo back in an `ack`; a client uses that to wait until the subscription is in effect.

```json
{"kind": "subscribe", "topic": "weather/current", "seq": 1}
```

There is no wildcard or pattern matching in this implementation. Subscribe to the exact topic string you want.

### `unsubscribe`

Removes the sender from `topic`. Like `subscribe`, it carries a `seq` the broker echoes in an `ack`.

```json
{"kind": "unsubscribe", "topic": "weather/current", "seq": 2}
```

Unsubscribing from a topic you never subscribed to is a no-op.

### `ping`

A no-op heartbeat. The broker decodes and discards it. Useful as a liveness check that doesn't perturb subscription state.

```json
{"kind": "ping"}
```

The broker does **not** send back a pong. Pings carry no `seq` and get no `ack`. If you want round-trip liveness, subscribe to a private topic and publish to it.

## Broker to client

Every frame sent from broker to client is a flat tagged union keyed on `kind`:

```json
{
  "kind":    "message" | "ack",
  "topic":   string (message only),
  "payload": any JSON value (message only),
  "seq":     unsigned integer (ack only)
}
```

### `message`

One delivered publish.

```json
{"kind": "message", "topic": "weather/current", "payload": {"temp_c": 21.4, "humidity": 65}}
```

`payload` is byte-for-byte the same JSON value the publisher sent. `topic` is the topic the publisher addressed.

### `ack`

Confirms that the broker applied a `subscribe` or `unsubscribe`. It echoes the `seq` from the request.

```json
{"kind": "ack", "seq": 1}
```

A client that sent a `subscribe` with `seq: 1` knows the subscription is in effect once it reads the matching `ack`. Unlike a delivery, an `ack` is best-effort: the broker sends it without blocking, so if the peer's outbound buffer happens to be full the `ack` is dropped and logged rather than stalling the broker loop. Subscribe and unsubscribe are idempotent, so a client whose `ack` was dropped can safely retry (its bounded wait will expire and it can re-issue).

## Delivery semantics

Delivery is backpressured. Each subscriber has a bounded outbound buffer on the broker side (`SubscriberBuffer`, default 256). The broker fans a publish out on the publishing connection's own reader goroutine, sending to each subscriber's buffer in turn. If a subscriber's buffer is full, that send blocks, which stops the broker reading the publisher's socket, which fills the publisher's kernel send buffer, which blocks the publisher's next write. The publisher is throttled to the slowest subscriber's rate; messages to a merely-slow subscriber are not dropped.

Two cases break that chain rather than block forever: a subscriber that **disconnects** during a fan-out is skipped, and a subscriber that **stops reading entirely** is evicted once a single broker-side write to it exceeds `WriteTimeout` (default 30s), at which point its connection is closed. A publisher whose `Publish` carries a `ctx` deadline stops waiting when that deadline fires.

Within a single subscriber, messages on the same topic from the same publisher arrive in publish order: one goroutine reads a publisher's frames and fans each out before reading the next, so that publisher's messages reach a subscriber's buffer in send order. Fan-out for different publishers runs on separate goroutines and is concurrent, so across publishers (or across topics) there is no global ordering.

Messages are not stored. A subscriber that subscribes after a publish has happened does not see the historical message.

## Error handling

Malformed frames close the connection on whichever side reads them. Same for frames over `MaxFrameSize`. There is no error frame.

If a client's first frame is not a valid `connect`, the broker closes the connection without sending anything.

A slow subscriber backpressures its publishers but keeps its connection. A subscriber that stops reading altogether is evicted once a broker-side write to it exceeds `WriteTimeout`: the broker closes that connection (and logs it) so a wedged peer cannot stall publishers indefinitely.

When a publisher disconnects, the broker scrubs its subscriptions on EOF. Messages that had already been fanned out still reach subscribers.

## Multi-language interop

A minimal Python client looks like this:

```python
import json
import socket
import struct

def write_frame(sock, value):
    body = json.dumps(value).encode("utf-8")
    sock.sendall(struct.pack(">I", len(body)) + body)

def read_frame(sock):
    header = recv_exactly(sock, 4)
    length = struct.unpack(">I", header)[0]
    return json.loads(recv_exactly(sock, length))

def recv_exactly(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk: raise EOFError
        buf += chunk
    return buf

sock = socket.create_connection(("127.0.0.1", 9000))
write_frame(sock, {"kind": "connect", "id": "py-client"})
write_frame(sock, {"kind": "subscribe", "topic": "weather/current", "seq": 1})
while True:
    frame = read_frame(sock)
    if frame["kind"] == "ack":
        # subscription with this seq is now in effect
        continue
    print(frame["topic"], frame["payload"])
```

There are no Go-isms in the protocol; any language with TCP and JSON can interoperate.
