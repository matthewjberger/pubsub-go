package pubsub

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

// ClientConfig configures a [Client] at dial time. ID is the peer name the
// broker registers under; it must be unique within a broker — reusing an
// ID evicts the previous connection. Address is the broker's host:port.
// InboxCapacity is the buffered size of the channel returned by [Inbox];
// zero means an unbuffered channel. Log is the destination for client
// diagnostics; if nil, a logger writing to [os.Stderr] is created.
type ClientConfig struct {
	ID            string
	Address       string
	InboxCapacity int
	Log           *log.Logger
}

// Client is the running state of a connected pub/sub peer. Construct one
// with [ConnectClient]. Outbound operations are package-level functions
// ([Publish], [Subscribe], [Unsubscribe], [Ping]); inbound messages are
// read from the channel returned by [Inbox]. The only methods on Client
// itself are lifecycle accessors ([Client.ID], [Client.Address],
// [Client.Close]).
type Client struct {
	id      string
	address string
	conn    net.Conn
	log     *log.Logger
	inbox   chan BrokerMessage

	writeMu sync.Mutex

	closeOnce sync.Once
	closed    chan struct{}
	readDone  chan struct{}
}

// ConnectClient dials the broker at cfg.Address, sends the connect
// handshake, and starts the reader goroutine. Returns once the handshake
// frame has been written.
func ConnectClient(cfg ClientConfig) (*Client, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("pubsub: ClientConfig.ID must be non-empty")
	}
	logger := cfg.Log
	if logger == nil {
		logger = log.New(os.Stderr, fmt.Sprintf("[client %s] ", cfg.ID), log.LstdFlags|log.Lmsgprefix)
	}
	conn, err := net.Dial("tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("pubsub: dial %s: %w", cfg.Address, err)
	}
	inboxCapacity := cfg.InboxCapacity
	if inboxCapacity < 0 {
		inboxCapacity = 0
	}
	c := &Client{
		id:       cfg.ID,
		address:  cfg.Address,
		conn:     conn,
		log:      logger,
		inbox:    make(chan BrokerMessage, inboxCapacity),
		closed:   make(chan struct{}),
		readDone: make(chan struct{}),
	}
	if err := writeEvent(c, PeerEvent{Kind: PeerEventConnect, ID: c.id}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pubsub: connect handshake: %w", err)
	}
	go runClientReader(c)
	logger.Printf("connected to %s", cfg.Address)
	return c, nil
}

// ID returns the peer ID this client registered with.
func (c *Client) ID() string { return c.id }

// Address returns the broker address this client is dialed against.
func (c *Client) Address() string { return c.address }

// Close shuts the client down: the TCP connection is closed (the broker
// sees EOF and drops the peer's subscriptions), the reader goroutine is
// drained, and the inbox is closed. Safe to call more than once.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		close(c.closed)
		err = c.conn.Close()
		<-c.readDone
		close(c.inbox)
	})
	if err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

// Inbox returns the channel of [BrokerMessage]s delivered to this client.
// The channel is closed when [Client.Close] is called or the connection
// drops. Range over it to consume messages.
func Inbox(c *Client) <-chan BrokerMessage {
	return c.inbox
}

// Publish marshals payload as JSON and sends it on topic. Use this when
// you have a Go value to send.
func Publish(c *Client, topic string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pubsub: marshal payload for %q: %w", topic, err)
	}
	return PublishRaw(c, topic, body)
}

// PublishRaw sends pre-encoded JSON on topic. Use this when the payload is
// already a [json.RawMessage] or when forwarding a payload received from
// elsewhere without re-encoding.
func PublishRaw(c *Client, topic string, payload json.RawMessage) error {
	return writeEvent(c, PeerEvent{
		Kind:    PeerEventPublish,
		ID:      c.id,
		Topic:   topic,
		Payload: payload,
	})
}

// Subscribe registers c to receive future publishes on topic. The
// subscription survives until [Unsubscribe] is called or the connection
// drops.
func Subscribe(c *Client, topic string) error {
	return writeEvent(c, PeerEvent{Kind: PeerEventSubscribe, ID: c.id, Topic: topic})
}

// Unsubscribe removes c from topic.
func Unsubscribe(c *Client, topic string) error {
	return writeEvent(c, PeerEvent{Kind: PeerEventUnsubscribe, ID: c.id, Topic: topic})
}

// Ping sends a no-op heartbeat. The broker accepts and ignores it; a
// failure here indicates the connection is gone.
func Ping(c *Client) error {
	return writeEvent(c, PeerEvent{Kind: PeerEventPing})
}

// writeEvent serialises event and writes one frame on the client's
// connection. Writes are mutex-serialised because publish/subscribe may
// be called from arbitrary goroutines.
func writeEvent(c *Client, event PeerEvent) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return net.ErrClosed
	default:
	}
	return WriteFrame(c.conn, event)
}

// runClientReader drains BrokerMessage frames from the connection onto
// the client's inbox channel. Exits on the first read error or when
// [Client.Close] is called.
func runClientReader(c *Client) {
	defer close(c.readDone)
	reader := bufio.NewReader(c.conn)
	for {
		var message BrokerMessage
		if err := ReadFrame(reader, &message); err != nil {
			if !isExpectedReadError(err) {
				select {
				case <-c.closed:
				default:
					c.log.Printf("read: %v", err)
				}
			}
			return
		}
		select {
		case c.inbox <- message:
		case <-c.closed:
			return
		}
	}
}

// isExpectedReadError reports whether err is one of the normal
// terminations (peer closed, local close) for the client reader.
func isExpectedReadError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}
