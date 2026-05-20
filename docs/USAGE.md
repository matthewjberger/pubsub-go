# Usage Guide

API reference for the `pubsub` package. Every exported identifier is listed here with the typical call pattern.

## Importing

```go
import "github.com/matthewjberger/pubsub-go/pubsub"
```

## Broker

### Start

```go
broker, err := pubsub.StartBroker(pubsub.BrokerConfig{
    Address: "127.0.0.1:9000",  // or "0.0.0.0:9000" or ":0" for a kernel-assigned port
    Log:     nil,               // nil falls back to stderr with "[broker] " prefix
})
if err != nil {
    log.Fatal(err)
}
defer broker.Shutdown()
```

`StartBroker` returns once the listener is up; the accept and broker loops are already running in goroutines.

### Address

```go
broker.Address()  // "127.0.0.1:51234" when bound with ":0"
```

Useful when you want a random free port (`Address: "127.0.0.1:0"`) and need to read back the actual port to share with clients.

### Shutdown

```go
broker.Shutdown()
```

Closes the listener, closes every peer connection, waits for the broker loop to drain. Safe to call more than once; the first call wins.

## Client

### Connect

```go
client, err := pubsub.ConnectClient(pubsub.ClientConfig{
    ID:            "weather-publisher",  // required, unique per broker
    Address:       broker.Address(),
    InboxCapacity: 64,                   // buffered inbox; 0 means unbuffered
    Log:           nil,                  // nil falls back to stderr with "[client <ID>] "
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

if err := pubsub.Publish(client, "weather/current", Weather{TempCelsius: 21.4, Humidity: 65}); err != nil {
    log.Printf("publish: %v", err)
}
```

`Publish` marshals the payload to JSON before sending. Use `PublishRaw` if you already have JSON bytes and want to avoid the re-encode:

```go
raw := json.RawMessage(`{"temp_c":21.4,"humidity":65}`)
pubsub.PublishRaw(client, "weather/current", raw)
```

### Subscribe / Unsubscribe

```go
pubsub.Subscribe(client, "weather/current")
pubsub.Subscribe(client, "weather/forecast")
// ...
pubsub.Unsubscribe(client, "weather/forecast")
```

There is no pattern matching; topics are exact strings.

### Receive

`Inbox` returns a `<-chan BrokerMessage`. Range over it or `select` on it.

```go
for message := range pubsub.Inbox(client) {
    fmt.Printf("%s -> %s\n", message.Topic, message.Payload)
}
```

The channel is closed when `client.Close()` is called or the connection drops. Range automatically exits on close.

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
pubsub.Ping(client)
```

A no-op heartbeat. The broker accepts and ignores it. Useful when you want a liveness check that doesn't change subscription state — an error return here means the connection is gone.

### Close

```go
client.Close()
```

Closes the TCP connection (the broker drops the peer's subscriptions), waits for the reader goroutine, then closes the inbox so any consumer ranging over `Inbox(client)` sees EOF. Safe to call more than once.

### Accessors

```go
client.ID()       // "weather-publisher"
client.Address()  // "127.0.0.1:9000"
```

## Wire types

Useful when you need to interoperate with non-Go peers or when you want to log frames.

### `PeerEvent`

```go
type PeerEvent struct {
    Kind    PeerEventKind   `json:"kind"`
    ID      string          `json:"id,omitempty"`
    Topic   string          `json:"topic,omitempty"`
    Payload json.RawMessage `json:"payload,omitempty"`
}
```

`Kind` is one of the `PeerEventConnect`, `PeerEventPublish`, `PeerEventSubscribe`, `PeerEventUnsubscribe`, `PeerEventPing` constants.

### `BrokerMessage`

```go
type BrokerMessage struct {
    Topic   string          `json:"topic"`
    Payload json.RawMessage `json:"payload"`
}
```

### Framing helpers

```go
pubsub.WriteFrame(writer, value)              // u32 BE length + JSON body
pubsub.ReadFrame(bufioReader, &target)        // reverse
pubsub.MaxFrameSize                           // 16 MiB
```

Use these if you want to write a custom client (test harness, fuzzing rig, third-party process).

## Concurrency rules

- `Publish`, `PublishRaw`, `Subscribe`, `Unsubscribe`, `Ping` are safe to call concurrently from multiple goroutines on the same `Client`. Writes are mutex-serialised on the way out.
- `Inbox(client)` returns a channel; multiple consumers ranging over it will compete for messages, which is fine.
- `Broker.Shutdown` is safe to call from multiple goroutines; only the first call does the shutdown.
- `Client.Close` is safe to call from multiple goroutines; only the first call does the close.
- A single `Client` value is meant for one logical peer. Sharing it across many goroutines is fine; making many `Client`s with the same ID is not (the broker evicts the older one).

## Patterns

### Reconnection wrapper

`Client` does not auto-reconnect; on disconnect, `Inbox` closes and writes return `net.ErrClosed`. Wrap `ConnectClient` in a loop if you want auto-reconnect:

```go
func runWithReconnect(ctx context.Context, cfg pubsub.ClientConfig, body func(*pubsub.Client)) {
    for ctx.Err() == nil {
        client, err := pubsub.ConnectClient(cfg)
        if err != nil {
            time.Sleep(2 * time.Second)
            continue
        }
        body(client)
        _ = client.Close()
    }
}
```

`body` re-subscribes and ranges over `Inbox(client)`; when the inbox closes, `body` returns and the outer loop reconnects.

### Typed topic helpers

The `Publish[T]`-style helpers from the Rust IPC library map cleanly to small per-topic functions in Go:

```go
func PublishWeather(client *pubsub.Client, w Weather) error {
    return pubsub.Publish(client, "weather/current", w)
}

func ConsumeWeather(message pubsub.BrokerMessage) (Weather, error) {
    var w Weather
    return w, json.Unmarshal(message.Payload, &w)
}
```

These are not part of the library; write them in your application code where you know the topic schema.

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

If you want per-topic channels, fan out in your own code; the library deliberately does not.
