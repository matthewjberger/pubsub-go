// Package pubsub is a small topic-based pub/sub broker and client that speak
// JSON over TCP.
//
// The wire types in [protocol.go] are plain data: [PeerEvent] from a client
// to the broker, [BrokerMessage] from the broker back to a subscriber. Each
// frame on the wire is a 4-byte big-endian length followed by exactly that
// many bytes of JSON, so any language with JSON and TCP can speak the
// protocol. No reflection, no codegen, no interfaces in the protocol.
//
// # Broker
//
// Start a broker with [StartBroker]; it returns once the listener is up. The
// broker accepts connections, demultiplexes [PeerEvent]s, and fans matching
// publishes out to every subscriber of a topic. Stop it with
// [Broker.Shutdown].
//
//	broker, err := pubsub.StartBroker(pubsub.BrokerConfig{Address: "127.0.0.1:9000"})
//	if err != nil { log.Fatal(err) }
//	defer broker.Shutdown()
//
// # Client
//
// Dial a broker with [ConnectClient]. Subscribe to topics, publish payloads
// of any JSON-serializable type, and read incoming messages off the inbox.
//
//	client, err := pubsub.ConnectClient(pubsub.ClientConfig{
//	    ID:      "demo",
//	    Address: "127.0.0.1:9000",
//	})
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
//	pubsub.Subscribe(client, "weather/current")
//	pubsub.Publish(client, "weather/current", map[string]any{"temp_c": 21.4})
//
//	for message := range pubsub.Inbox(client) {
//	    fmt.Printf("%s -> %s\n", message.Topic, message.Payload)
//	}
//
// # Design
//
// State is held in plain structs ([Broker], [Client]); all behaviour is in
// package-level functions that operate on them. Each peer connection on the
// broker is owned by a dedicated reader goroutine that pushes decoded events
// into a single broker loop; the loop is the only place that mutates the
// subscription map. Each peer also has a writer goroutine that drains its
// per-peer outbound channel onto the socket. This is the same shape as the
// reference Rust IPC broker, minus the bridging, deferred publishing, and
// scheduling layers.
package pubsub
