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

The protocol is duplex and asynchronous. Publishes are fire-and-forget. Subscribes and unsubscribes carry a `seq` that the broker echoes back in an `ack`, which is the only request/response correlation in the protocol; a client uses it to make `subscribe` synchronous so a publish cannot race ahead of a subscription.

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

`payload` can be any JSON value (object, array, string, number, boolean, null). Publishing is fire-and-forget; the broker sends no acknowledgement.

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

A client that sent a `subscribe` with `seq: 1` knows the subscription is in effect once it reads the matching `ack`. Acknowledgements are best-effort like deliveries: if the peer's outbound buffer is full the broker drops the `ack` and logs it. Subscribe and unsubscribe are idempotent, so a client whose `ack` was dropped can safely retry.

## Delivery semantics

Delivery is best-effort, at-most-once. Each subscriber has a bounded outbound buffer on the broker side. If a subscriber is too slow to drain, the broker drops the message for that subscriber and writes one log line. No retry, no NAK, no per-subscriber backpressure to the publisher. A dropped message is gone.

Within a single subscriber, messages on the same topic from the same publisher arrive in publish order. Across topics or across publishers there is no global ordering. The broker processes events from one peer at a time, but other peers may interleave between any two of yours.

Messages are not stored. A subscriber that subscribes after a publish has happened does not see the historical message.

## Error handling

Malformed frames close the connection on whichever side reads them. Same for frames over `MaxFrameSize`. There is no error frame.

If a client's first frame is not a valid `connect`, the broker closes the connection without sending anything.

A slow subscriber gets its message dropped (and logged broker-side) but its connection stays open. The broker does not retaliate against the subscriber.

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
