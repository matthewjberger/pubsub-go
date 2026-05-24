# Usage Guide

API reference for the `pubsub` package. Every exported identifier is listed here with the typical call pattern.

## Importing

```go
import "github.com/matthewjberger/pubsub-go/pubsub"
```

## Context

Every operation takes a `context.Context` as its first argument. It bounds the work and lets a caller cancel:

- `ConnectClient` uses it for the TCP dial and the handshake write.
- `Publish`, `PublishRaw`, and `Ping` use it to set a write deadline on the socket. With a plain `context.Background()` there is no deadline.
- `Subscribe` and `Unsubscribe` use it to bound the wait for the broker's acknowledgement.

Pass `context.Background()` when you do not need a deadline, or a `context.WithTimeout` when you do.

## Broker

### Start

```go
broker, err := pubsub.StartBroker(pubsub.BrokerConfig{
    Address:          "127.0.0.1:9000", // or "0.0.0.0:9000" or ":0" for a kernel-assigned port
    Log:              nil,              // nil falls back to stderr with "[broker] " prefix
    WriteTimeout:     0,                // 0 uses DefaultWriteTimeout (30s)
    SubscriberBuffer: 0,                // 0 uses DefaultSubscriberBuffer (256)
})
if err != nil {
    log.Fatal(err)
}
defer broker.Shutdown()
```

`StartBroker` returns once the listener is up. The accept and broker loops are already running in goroutines. `WriteTimeout` bounds a single socket write to one peer. A write that blocks past the timeout drops that connection so its writer goroutine cannot leak.

`SubscriberBuffer` is the per-subscriber outbound queue depth. It sets how far a publisher may run ahead of a subscriber before publishing backpressures: once a subscriber's queue is full, the publisher delivering to it blocks (see [Publish](#publish)). A larger buffer absorbs more burst before throttling at the cost of more queued memory per subscriber.

### Address

```go
broker.Address() // "127.0.0.1:51234" when bound with ":0"
```

Useful when you want a random free port (`Address: "127.0.0.1:0"`) and need to read back the actual port to share with clients.

### Shutdown

```go
broker.Shutdown()
```

Closes the listener, closes every peer connection, and waits for the broker loop to drain. Safe to call more than once. The first call wins.

## Client

### Connect

```go
ctx := context.Background()

client, err := pubsub.ConnectClient(ctx, pubsub.ClientConfig{
    ID:            "weather-publisher", // required, unique per broker
    Address:       broker.Address(),
    InboxCapacity: 64,                  // buffered inbox; 0 means unbuffered
    Log:           nil,                 // nil falls back to stderr with "[client <ID>] "
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

`ConnectClient` dials TCP, sends the connect handshake, and starts the reader goroutine before returning. If a peer with the same ID is already connected to the broker, the previous connection is dropped.

### Publish

```go
type Weather struct {
    TempCelsius float64 `json:"temp_c"`
    Humidity    float64 `json:"humidity"`
}

if err := pubsub.Publish(ctx, client, "weather/current", Weather{TempCelsius: 21.4, Humidity: 65}); err != nil {
    log.Printf("publish: %v", err)
}
```

`Publish` marshals the payload to JSON before sending. A nil return means the broker accepted the frame for fan-out to the subscribers connected at that moment, not that any application has yet read it off its inbox. Delivery is backpressured, not best-effort: if a subscriber is too slow to drain, the broker stops reading this publisher's socket, and a subsequent `Publish` blocks until the slow subscriber catches up or the call's `ctx` deadline fires. Pass a `ctx` with a timeout if you do not want a publish to block indefinitely behind a stuck subscriber. Use `PublishRaw` if you already have JSON bytes and want to avoid the re-encode:

```go
raw := json.RawMessage(`{"temp_c":21.4,"humidity":65}`)
pubsub.PublishRaw(ctx, client, "weather/current", raw)
```

### Subscribe / Unsubscribe

```go
pubsub.Subscribe(ctx, client, "weather/current")
pubsub.Subscribe(ctx, client, "weather/forecast")
// ...
pubsub.Unsubscribe(ctx, client, "weather/forecast")
```

Both are synchronous. They block until the broker acknowledges the change, `ctx` is cancelled, or the client is closed. A nil return from `Subscribe` means the broker has applied the subscription, so a publish that follows is delivered. There is no subscribe/publish race to sleep around. Both are idempotent: subscribing twice or unsubscribing from a topic you never had is a no-op on the broker, and a retry after a cancelled call is safe.

There is no pattern matching. Topics are exact strings.

### Receive

`Inbox` returns a `<-chan BrokerMessage`. Range over it or `select` on it.

```go
for message := range pubsub.Inbox(client) {
    fmt.Printf("%s -> %s\n", message.Topic, message.Payload)
}
```

The channel is closed when `client.Close()` is called or the connection drops. Range automatically exits on close. Acknowledgement frames never reach the inbox; only delivered publishes do.

`message.Payload` is a `json.RawMessage`. Unmarshal it into your application type:

```go
var weather Weather
if err := json.Unmarshal(message.Payload, &weather); err != nil {
    log.Printf("unmarshal weather: %v", err)
    continue
}
```

### Ping

```go
pubsub.Ping(ctx, client)
```

A no-op heartbeat. The broker accepts and ignores it. Useful when you want a liveness check that does not change subscription state. An error return here means the connection is gone.

### Close

```go
client.Close()
```

Closes the TCP connection (the broker drops the peer's subscriptions), waits for the reader goroutine, then closes the inbox so any consumer ranging over `Inbox(client)` sees EOF. Any `Subscribe` or `Unsubscribe` still waiting for an acknowledgement is released with `ErrClosed`. Safe to call more than once.

### Accessors

```go
client.ID()      // "weather-publisher"
client.Address() // "127.0.0.1:9000"
```

### ErrClosed

```go
if errors.Is(err, pubsub.ErrClosed) {
    // the client was closed; reconnect if you want to continue
}
```

Any operation attempted after `Close` returns `pubsub.ErrClosed`. It is distinct from `net.ErrClosed` so you can tell "you closed this client" apart from a lower-level socket error.

## Wire types

Useful when you need to interoperate with non-Go peers or when you want to log frames.

### `PeerEvent`

```go
type PeerEvent struct {
    Kind    PeerEventKind   `json:"kind"`
    ID      string          `json:"id,omitempty"`
    Topic   string          `json:"topic,omitempty"`
    Payload json.RawMessage `json:"payload,omitempty"`
    Seq     uint64          `json:"seq,omitempty"`
}
```

`Kind` is one of the `PeerEventConnect`, `PeerEventPublish`, `PeerEventSubscribe`, `PeerEventUnsubscribe`, `PeerEventPing` constants. `ID` is only set on the connect frame. `Seq` is set on subscribe and unsubscribe, and the broker echoes it back in an acknowledgement.

### `BrokerMessage`

```go
type BrokerMessage struct {
    Topic   string          `json:"topic"`
    Payload json.RawMessage `json:"payload"`
}
```

This is the decoded value you read off `Inbox`. On the wire the broker sends a tagged frame (`{"kind":"message",...}` for a delivery, `{"kind":"ack","seq":N}` for an acknowledgement); the client decodes message frames into `BrokerMessage` and routes acknowledgement frames internally. See [`PROTOCOL.md`](PROTOCOL.md) for the exact wire shape.

### Framing helpers

```go
pubsub.WriteFrame(writer, value)       // u32 BE length + JSON body
pubsub.ReadFrame(bufioReader, &target) // reverse
pubsub.MaxFrameSize                    // 16 MiB
```

Use these if you want to write a custom client (test harness, fuzzing rig, third-party process).

## Concurrency rules

- `Publish`, `PublishRaw`, `Subscribe`, `Unsubscribe`, `Ping` are safe to call concurrently from multiple goroutines on the same `Client`. Writes are mutex-serialised on the way out, and acknowledgements are matched by sequence number, so concurrent subscribes do not confuse each other's replies.
- `Inbox(client)` returns a channel. Multiple consumers ranging over it will compete for messages, which is fine.
- `Broker.Shutdown` is safe to call from multiple goroutines. Only the first call does the shutdown.
- `Client.Close` is safe to call from multiple goroutines. Only the first call does the close.
- A single `Client` value is meant for one logical peer. Sharing it across many goroutines is fine. Making many `Client`s with the same ID is not, because the broker evicts the older one.

## Patterns

### Reconnection wrapper

`Client` does not auto-reconnect. On disconnect, `Inbox` closes and writes return `ErrClosed`. Wrap `ConnectClient` in a loop if you want auto-reconnect:

```go
func runWithReconnect(ctx context.Context, cfg pubsub.ClientConfig, body func(context.Context, *pubsub.Client)) {
    for ctx.Err() == nil {
        client, err := pubsub.ConnectClient(ctx, cfg)
        if err != nil {
            time.Sleep(2 * time.Second)
            continue
        }
        body(ctx, client)
        _ = client.Close()
    }
}
```

`body` re-subscribes and ranges over `Inbox(client)`. When the inbox closes, `body` returns and the outer loop reconnects.

### Typed topic helpers

Wrap the topic string and the payload type in small per-topic functions where you know the schema:

```go
func PublishWeather(ctx context.Context, client *pubsub.Client, w Weather) error {
    return pubsub.Publish(ctx, client, "weather/current", w)
}

func ConsumeWeather(message pubsub.BrokerMessage) (Weather, error) {
    var w Weather
    return w, json.Unmarshal(message.Payload, &w)
}
```

These are not part of the library. Write them in your application code where you know the topic schema.

### Multiple subscriptions, one inbox

There is one inbox per client. Multiplex topics by switching on `message.Topic`:

```go
for message := range pubsub.Inbox(client) {
    switch message.Topic {
    case "weather/current":
        var w Weather
        json.Unmarshal(message.Payload, &w)
        handleWeather(w)
    case "weather/forecast":
        // ...
    }
}
```

If you want per-topic channels, fan out in your own code. The library deliberately does not.
