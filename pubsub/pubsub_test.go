package pubsub

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
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
	return connectWithInbox(t, broker, id, 16)
}

func connectWithInbox(t *testing.T, broker *Broker, id string, inboxCapacity int) *Client {
	t.Helper()
	client, err := ConnectClient(testContext(t), ClientConfig{
		ID:            id,
		Address:       broker.Address(),
		InboxCapacity: inboxCapacity,
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

// TestConcurrentDeliveryIsLossless runs many publishers against many
// subscribers with intentionally small inboxes, so delivery is constantly
// backpressured. Every subscriber must receive every published message
// exactly once: no drops (backpressure, not best-effort) and no duplicates.
// Run under -race to also exercise the goroutine interleavings.
func TestConcurrentDeliveryIsLossless(t *testing.T) {
	broker := startBrokerOnEphemeralPort(t)

	const publishers = 6
	const subscribers = 6
	const perPublisher = 40
	const topic = "load"
	total := publishers * perPublisher

	subClients := make([]*Client, subscribers)
	for index := range subClients {
		client := connectWithInbox(t, broker, fmt.Sprintf("sub-%d", index), 4)
		if err := Subscribe(testContext(t), client, topic); err != nil {
			t.Fatalf("Subscribe sub-%d: %v", index, err)
		}
		subClients[index] = client
	}

	pubClients := make([]*Client, publishers)
	for index := range pubClients {
		pubClients[index] = connect(t, broker, fmt.Sprintf("pub-%d", index))
	}

	received := make([]map[int]int, subscribers)
	var consumers sync.WaitGroup
	for index, client := range subClients {
		received[index] = map[int]int{}
		consumers.Add(1)
		go func(index int, client *Client) {
			defer consumers.Done()
			for got := range total {
				select {
				case message, ok := <-Inbox(client):
					if !ok {
						t.Errorf("sub-%d inbox closed after %d/%d", index, got, total)
						return
					}
					var value int
					if err := json.Unmarshal(message.Payload, &value); err != nil {
						t.Errorf("sub-%d unmarshal: %v", index, err)
						return
					}
					received[index][value]++
				case <-time.After(15 * time.Second):
					t.Errorf("sub-%d timed out after %d/%d messages", index, got, total)
					return
				}
			}
		}(index, client)
	}

	var producers sync.WaitGroup
	for index, client := range pubClients {
		producers.Add(1)
		go func(base int, client *Client) {
			defer producers.Done()
			for offset := range perPublisher {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				err := Publish(ctx, client, topic, base*perPublisher+offset)
				cancel()
				if err != nil {
					t.Errorf("pub-%d publish: %v", base, err)
					return
				}
			}
		}(index, client)
	}

	producers.Wait()
	consumers.Wait()

	for index := range received {
		if len(received[index]) != total {
			t.Fatalf("sub-%d received %d distinct values, want %d", index, len(received[index]), total)
		}
		for value, count := range received[index] {
			if count != 1 {
				t.Fatalf("sub-%d received value %d %d times, want exactly 1", index, value, count)
			}
		}
	}
}

// TestSlowSubscriberReceivesEveryMessageInOrder drains a subscriber slower
// than a publisher produces, forcing the per-subscriber buffer full and the
// publisher to backpressure. The subscriber must still see every message, in
// publish order, with none dropped.
func TestSlowSubscriberReceivesEveryMessageInOrder(t *testing.T) {
	broker := startBrokerOnEphemeralPort(t)
	subscriber := connectWithInbox(t, broker, "slow", 1)
	publisher := connect(t, broker, "fast")

	if err := Subscribe(testContext(t), subscriber, "topic"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const count = 30
	go func() {
		for value := range count {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := Publish(ctx, publisher, "topic", value)
			cancel()
			if err != nil {
				t.Errorf("publish %d: %v", value, err)
				return
			}
		}
	}()

	for want := range count {
		select {
		case message := <-Inbox(subscriber):
			var value int
			if err := json.Unmarshal(message.Payload, &value); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if value != want {
				t.Fatalf("message %d = %d, want %d (loss or reordering)", want, value, want)
			}
			time.Sleep(5 * time.Millisecond)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for message %d", want)
		}
	}
}

// TestWriteTimeoutEvictsStuckSubscriber connects a subscriber that subscribes
// and then never reads its socket. With a short write timeout, the broker's
// per-peer write deadline must fire and the broker must close (evict) that
// connection, rather than wedging forever.
func TestWriteTimeoutEvictsStuckSubscriber(t *testing.T) {
	broker, err := StartBroker(BrokerConfig{
		Address:          "127.0.0.1:0",
		Log:              silentLogger(),
		WriteTimeout:     300 * time.Millisecond,
		SubscriberBuffer: 1,
	})
	if err != nil {
		t.Fatalf("StartBroker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Shutdown() })

	stuck := dialRaw(t, broker.Address(), "stuck")
	stuckReader := bufio.NewReader(stuck)
	if err := WriteFrame(stuck, PeerEvent{Kind: PeerEventSubscribe, Topic: "flood", Seq: 1}); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	var ack brokerFrame
	if err := ReadFrame(stuckReader, &ack); err != nil || ack.Kind != brokerFrameAck {
		t.Fatalf("expected subscribe ack, got %+v err %v", ack, err)
	}
	// stuck deliberately stops reading from here on.

	publisher := connect(t, broker, "flood-pub")
	payload := strings.Repeat("x", 16*1024)
	stop := make(chan struct{})
	var flood sync.WaitGroup
	flood.Add(1)
	go func() {
		defer flood.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = Publish(ctx, publisher, "flood", payload)
			cancel()
		}
	}()
	defer func() {
		close(stop)
		flood.Wait()
	}()

	// Let the broker's write deadline fire and close the stuck connection
	// before we start reading, so the eviction is what ends the reads.
	time.Sleep(1500 * time.Millisecond)
	_ = stuck.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 32*1024)
	evicted := false
	for {
		if _, err := stuck.Read(buf); err != nil {
			evicted = !errors.Is(err, os.ErrDeadlineExceeded)
			break
		}
	}
	if !evicted {
		t.Fatal("broker did not evict the stuck subscriber within the write timeout")
	}
}

// TestReadFrameRejectsOversizeLength checks the frame guard rejects a length
// prefix beyond MaxFrameSize without allocating the body.
func TestReadFrameRejectsOversizeLength(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], MaxFrameSize+1)
	reader := bufio.NewReader(bytes.NewReader(header[:]))
	var target PeerEvent
	if err := ReadFrame(reader, &target); err == nil {
		t.Fatal("ReadFrame accepted an oversize length, want error")
	}
}

// TestReadFrameRejectsZeroLength checks the frame guard rejects a zero-length
// frame.
func TestReadFrameRejectsZeroLength(t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader([]byte{0, 0, 0, 0}))
	var target PeerEvent
	if err := ReadFrame(reader, &target); err == nil {
		t.Fatal("ReadFrame accepted a zero-length frame, want error")
	}
}

// TestFrameRoundTrip checks a frame written by WriteFrame decodes back to an
// equal value through ReadFrame.
func TestFrameRoundTrip(t *testing.T) {
	original := PeerEvent{Kind: PeerEventPublish, Topic: "weather/current", Payload: json.RawMessage(`{"temp_c":21.4}`)}
	var buffer bytes.Buffer
	if err := WriteFrame(&buffer, original); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	var decoded PeerEvent
	if err := ReadFrame(bufio.NewReader(&buffer), &decoded); err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if decoded.Kind != original.Kind || decoded.Topic != original.Topic || string(decoded.Payload) != string(original.Payload) {
		t.Fatalf("round trip mismatch: got %+v want %+v", decoded, original)
	}
}

// dialRaw opens a TCP connection to the broker and sends the connect
// handshake, returning the raw connection for tests that need to drive the
// wire protocol directly.
func dialRaw(t *testing.T, address, id string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := WriteFrame(conn, PeerEvent{Kind: PeerEventConnect, ID: id}); err != nil {
		t.Fatalf("write connect handshake: %v", err)
	}
	return conn
}
