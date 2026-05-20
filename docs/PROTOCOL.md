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
3. **Operations.** The client sends any sequence of `PeerEvent` frames: `publish`, `subscribe`, `unsubscribe`, `ping`. The broker may send `BrokerMessage` frames at any time after the handshake completes (one for every published message that matches a topic the client subscribed to).
4. **Close.** Either side closes the TCP connection. The broker drops the peer from its subscription tables on EOF; the client drains any buffered inbound messages and closes its inbox channel.

The protocol is duplex and asynchronous. There is no request/response correlation; subscribes and publishes are fire-and-forget.

## Client to broker: `PeerEvent`

`PeerEvent` is a flat tagged union. `kind` selects which other fields are meaningful; fields that do not apply to a `kind` are omitted from the JSON.

```json
{
  "kind":      "connect" | "publish" | "subscribe" | "unsubscribe" | "ping",
  "id":        string (optional),
  "topic":     string (optional),
  "payload":   any JSON value (optional)
}
```

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
  "id":      "weather-publisher",
  "topic":   "weather/current",
  "payload": {"temp_c": 21.4, "humidity": 65}
}
```

`payload` can be any JSON value (object, array, string, number, boolean, null).

### `subscribe`

Subscribes the sender to `topic`. Subsequent publishes to that topic are delivered to this peer. Subscribing to the same topic twice is a no-op.

```json
{"kind": "subscribe", "id": "weather-subscriber", "topic": "weather/current"}
```

There is no wildcard or pattern matching in this implementation. Subscribe to the exact topic string you want.

### `unsubscribe`

Removes the sender from `topic`.

```json
{"kind": "unsubscribe", "id": "weather-subscriber", "topic": "weather/current"}
```

Unsubscribing from a topic you never subscribed to is a no-op.

### `ping`

A no-op heartbeat. The broker decodes and discards it. Useful as a liveness check that doesn't perturb subscription state.

```json
{"kind": "ping"}
```

The broker does **not** send back a pong. Pings are one-way; if you want round-trip liveness, subscribe to a private topic and publish to it.

## Broker to client: `BrokerMessage`

Every frame sent from broker to client is a `BrokerMessage`:

```json
{
  "topic":   string,
  "payload": any JSON value
}
```

```json
{"topic": "weather/current", "payload": {"temp_c": 21.4, "humidity": 65}}
```

`payload` is byte-for-byte the same JSON value the publisher sent. `topic` is the topic the publisher addressed.

## Delivery semantics

- **Best-effort.** Each subscriber has a bounded outbound buffer on the broker side. If a subscriber is too slow to drain, messages are dropped for that subscriber (with a broker-side log line). No retry, no NAK, no per-subscriber backpressure to the publisher.
- **At-most-once.** A message that is dropped for a slow subscriber is dropped, not retried.
- **No ordering across topics.** Within a single subscriber, messages on the same topic from the same publisher arrive in publish order. Across topics or across publishers, no global ordering is guaranteed (the broker processes events from one peer at a time but other peers may interleave between any two of yours).
- **No persistence.** Messages are not stored. A subscriber that subscribes after a publish has happened does not see the historical message.

## Error handling

- **Malformed frame.** Either side closes the connection. There is no error frame.
- **Frame too large.** Either side closes the connection.
- **Wrong first frame.** The broker closes the connection without sending anything.
- **Slow subscriber.** The broker drops the message for that subscriber only and logs it. The subscriber's connection stays open.
- **Disconnected publisher.** The broker scrubs the publisher's subscriptions on EOF. Already-fanned-out messages still reach subscribers.

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
write_frame(sock, {"kind": "subscribe", "id": "py-client", "topic": "weather/current"})
while True:
    print(read_frame(sock))
```

There are no Go-isms in the protocol; any language with TCP and JSON can interoperate.
