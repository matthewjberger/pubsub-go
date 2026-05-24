package pubsub

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"testing"
	"time"
)

// silentLogger discards broker/client diagnostics during tests.
func silentLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

// testContext returns a context bounded so a stuck call fails the test
// instead of hanging the suite.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func startBrokerOnEphemeralPort(t *testing.T) *Broker {
	t.Helper()
	broker, err := StartBroker(BrokerConfig{Address: "127.0.0.1:0", Log: silentLogger()})
	if err != nil {
		t.Fatalf("StartBroker: %v", err)
	}
	t.Cleanup(func() {
		_ = broker.Shutdown()
	})
	return broker
}

func connect(t *testing.T, broker *Broker, id string) *Client {
	t.Helper()
	client, err := ConnectClient(testContext(t), ClientConfig{
		ID:            id,
		Address:       broker.Address(),
		InboxCapacity: 16,
		Log:           silentLogger(),
	})
	if err != nil {
		t.Fatalf("ConnectClient: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func TestPublishSubscribeRoundTrip(t *testing.T) {
	broker := startBrokerOnEphemeralPort(t)

	subscriber := connect(t, broker, "subscriber")
	publisher := connect(t, broker, "publisher")

	// Subscribe is synchronous: when it returns, the broker has recorded
	// the subscription, so the publish below cannot race ahead of it.
	if err := Subscribe(testContext(t), subscriber, "weather/current"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	type weather struct {
		TempCelsius float64 `json:"temp_c"`
	}
	if err := Publish(testContext(t), publisher, "weather/current", weather{TempCelsius: 21.4}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case message := <-Inbox(subscriber):
		if message.Topic != "weather/current" {
			t.Fatalf("topic = %q, want %q", message.Topic, "weather/current")
		}
		var got weather
		if err := json.Unmarshal(message.Payload, &got); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if got.TempCelsius != 21.4 {
			t.Fatalf("temp_c = %v, want %v", got.TempCelsius, 21.4)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for published message")
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	broker := startBrokerOnEphemeralPort(t)
	subscriber := connect(t, broker, "subscriber")
	publisher := connect(t, broker, "publisher")

	if err := Subscribe(testContext(t), subscriber, "topic"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := Publish(testContext(t), publisher, "topic", "first"); err != nil {
		t.Fatalf("Publish first: %v", err)
	}

	select {
	case message := <-Inbox(subscriber):
		var got string
		if err := json.Unmarshal(message.Payload, &got); err != nil || got != "first" {
			t.Fatalf("first delivery payload = %q err = %v", got, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive first message")
	}

	if err := Unsubscribe(testContext(t), subscriber, "topic"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	if err := Publish(testContext(t), publisher, "topic", "second"); err != nil {
		t.Fatalf("Publish second: %v", err)
	}

	select {
	case message := <-Inbox(subscriber):
		t.Fatalf("got unexpected message after unsubscribe: %+v", message)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestDuplicateIDReconnectKeepsNewPeerLive(t *testing.T) {
	// Regression test for the stale-disconnect race: when a peer with the
	// same ID reconnects, the old reader's eventual disconnect event must
	// not evict the new peer from the broker's peer map or its
	// subscriptions.
	broker := startBrokerOnEphemeralPort(t)

	first, err := ConnectClient(testContext(t), ClientConfig{
		ID:            "shared-id",
		Address:       broker.Address(),
		InboxCapacity: 16,
		Log:           silentLogger(),
	})
	if err != nil {
		t.Fatalf("ConnectClient first: %v", err)
	}
	// A synchronous subscribe doubles as a barrier: its ack proves the
	// broker has processed the first connection before the second dials.
	if err := Subscribe(testContext(t), first, "barrier"); err != nil {
		t.Fatalf("Subscribe first: %v", err)
	}

	second, err := ConnectClient(testContext(t), ClientConfig{
		ID:            "shared-id",
		Address:       broker.Address(),
		InboxCapacity: 16,
		Log:           silentLogger(),
	})
	if err != nil {
		t.Fatalf("ConnectClient second: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	// The first connection is now evicted server-side; closing the
	// stranded socket triggers the old reader's stale disconnect.
	_ = first.Close()

	// The ack for this subscribe proves the broker applied it after the
	// new connect. With the stale-disconnect bug, the first reader's
	// disconnect would have evicted the new peer and this subscription
	// would be lost.
	if err := Subscribe(testContext(t), second, "topic"); err != nil {
		t.Fatalf("Subscribe second: %v", err)
	}

	publisher, err := ConnectClient(testContext(t), ClientConfig{
		ID:      "publisher",
		Address: broker.Address(),
		Log:     silentLogger(),
	})
	if err != nil {
		t.Fatalf("ConnectClient publisher: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	if err := Publish(testContext(t), publisher, "topic", "hello"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case message := <-Inbox(second):
		var got string
		if err := json.Unmarshal(message.Payload, &got); err != nil || got != "hello" {
			t.Fatalf("payload = %q err = %v", got, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second client did not receive message; stale disconnect evicted the new peer")
	}
}

func TestPingDoesNotEcho(t *testing.T) {
	broker := startBrokerOnEphemeralPort(t)
	client := connect(t, broker, "pinger")
	if err := Ping(testContext(t), client); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	select {
	case message := <-Inbox(client):
		t.Fatalf("ping produced a delivery: %+v", message)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestSubscribeAfterCloseReturnsErrClosed(t *testing.T) {
	broker := startBrokerOnEphemeralPort(t)
	client := connect(t, broker, "closer")
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := Subscribe(testContext(t), client, "topic"); err != ErrClosed {
		t.Fatalf("Subscribe after close = %v, want %v", err, ErrClosed)
	}
}
