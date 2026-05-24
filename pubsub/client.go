package pubsub

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by client operations attempted after [Client.Close]
// has run. It is distinct from [net.ErrClosed] so callers can tell "you
// closed this client" apart from a lower-level socket error.
var ErrClosed = errors.New("pubsub: client closed")

// ClientConfig configures a [Client] at dial time. ID is the peer name the
// broker registers under; it must be unique within a broker, since reusing
// an ID evicts the previous connection. Address is the broker's host:port.
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
// itself are lifecycle ([Client.Close]) and trivial accessors
// ([Client.ID], [Client.Address]).
type Client struct {
	id      string
	address string
	conn    net.Conn
	log     *log.Logger
	inbox   chan BrokerMessage

	writeMu sync.Mutex

	seq       atomic.Uint64
	pendingMu sync.Mutex
	pending   map[uint64]chan struct{}

	closeOnce sync.Once
	closed    chan struct{}
	readDone  chan struct{}
}

// ConnectClient dials the broker at cfg.Address, sends the connect
// handshake, and starts the reader goroutine. The dial and the handshake
// write both honour ctx: a cancelled or expired ctx aborts the connect.
// Returns once the handshake frame has been written.
func ConnectClient(ctx context.Context, cfg ClientConfig) (*Client, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("pubsub: ClientConfig.ID must be non-empty")
	}
	logger := cfg.Log
	if logger == nil {
		logger = log.New(os.Stderr, fmt.Sprintf("[client %s] ", cfg.ID), log.LstdFlags|log.Lmsgprefix)
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("pubsub: dial %s: %w", cfg.Address, err)
	}
	inboxCapacity := max(cfg.InboxCapacity, 0)
	c := &Client{
		id:       cfg.ID,
		address:  cfg.Address,
		conn:     conn,
		log:      logger,
		inbox:    make(chan BrokerMessage, inboxCapacity),
		pending:  map[uint64]chan struct{}{},
		closed:   make(chan struct{}),
		readDone: make(chan struct{}),
	}
	if err := writeEvent(ctx, c, PeerEvent{Kind: PeerEventConnect, ID: c.id}); err != nil {
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
// drained, and the inbox is closed. Any [Subscribe] or [Unsubscribe] call
// still waiting for an acknowledgement is released with [ErrClosed]. Safe
// to call more than once.
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
// you have a Go value to send. Publishing is fire-and-forget: a nil return
// means the frame was written, not that any subscriber received it. ctx
// bounds the write with a deadline if it carries one.
func Publish(ctx context.Context, c *Client, topic string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pubsub: marshal payload for %q: %w", topic, err)
	}
	return PublishRaw(ctx, c, topic, body)
}

// PublishRaw sends pre-encoded JSON on topic. Use this when the payload is
// already a [json.RawMessage] or when forwarding a payload received from
// elsewhere without re-encoding.
func PublishRaw(ctx context.Context, c *Client, topic string, payload json.RawMessage) error {
	return writeEvent(ctx, c, PeerEvent{
		Kind:    PeerEventPublish,
		Topic:   topic,
		Payload: payload,
	})
}

// Subscribe registers c to receive future publishes on topic and blocks
// until the broker acknowledges the subscription, ctx is cancelled, or the
// client is closed. A nil return means the broker has applied the
// subscription, so a publish sent afterwards by anyone will be delivered;
// there is no subscribe/publish race to sleep around. The subscription
// survives until [Unsubscribe] is called or the connection drops.
func Subscribe(ctx context.Context, c *Client, topic string) error {
	return awaitAck(ctx, c, PeerEvent{Kind: PeerEventSubscribe, Topic: topic})
}

// Unsubscribe removes c from topic and blocks until the broker
// acknowledges, ctx is cancelled, or the client is closed.
func Unsubscribe(ctx context.Context, c *Client, topic string) error {
	return awaitAck(ctx, c, PeerEvent{Kind: PeerEventUnsubscribe, Topic: topic})
}

// Ping sends a no-op heartbeat. The broker accepts and ignores it; a
// failure here indicates the connection is gone. ctx bounds the write.
func Ping(ctx context.Context, c *Client) error {
	return writeEvent(ctx, c, PeerEvent{Kind: PeerEventPing})
}

// awaitAck stamps event with the next sequence number, registers a waiter
// for that sequence, writes the frame, and blocks until the broker echoes
// the sequence in an acknowledgement. Subscribe and Unsubscribe use it so a
// successful return means the broker has applied the change. The
// acknowledgement is matched by sequence, so concurrent calls do not
// confuse each other's replies.
func awaitAck(ctx context.Context, c *Client, event PeerEvent) error {
	sequence := c.seq.Add(1)
	event.Seq = sequence
	ack := make(chan struct{})
	c.pendingMu.Lock()
	c.pending[sequence] = ack
	c.pendingMu.Unlock()

	if err := writeEvent(ctx, c, event); err != nil {
		forgetPending(c, sequence)
		return err
	}

	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		forgetPending(c, sequence)
		return ctx.Err()
	case <-c.closed:
		forgetPending(c, sequence)
		return ErrClosed
	}
}

// forgetPending drops a pending acknowledgement waiter. The reader deletes
// the entry when it delivers an ack; this covers the cancel/close paths so
// the map does not leak abandoned waiters.
func forgetPending(c *Client, sequence uint64) {
	c.pendingMu.Lock()
	delete(c.pending, sequence)
	c.pendingMu.Unlock()
}

// writeEvent serialises event and writes one frame on the client's
// connection. Writes are mutex-serialised because publish/subscribe may
// be called from arbitrary goroutines. If ctx carries a deadline it is set
// on the socket for the duration of the write.
func writeEvent(ctx context.Context, c *Client, event PeerEvent) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return ErrClosed
	default:
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
		defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	}
	return WriteFrame(c.conn, event)
}

// runClientReader drains broker frames from the connection. Message frames
// become [BrokerMessage]s on the inbox; ack frames release the matching
// [awaitAck] waiter. Exits on the first read error or when [Client.Close]
// is called.
func runClientReader(c *Client) {
	defer close(c.readDone)
	reader := bufio.NewReader(c.conn)
	for {
		var frame brokerFrame
		if err := ReadFrame(reader, &frame); err != nil {
			if !isExpectedReadError(err) {
				select {
				case <-c.closed:
				default:
					c.log.Printf("read: %v", err)
				}
			}
			return
		}
		switch frame.Kind {
		case brokerFrameAck:
			deliverAck(c, frame.Seq)
		case brokerFrameMessage:
			select {
			case c.inbox <- BrokerMessage{Topic: frame.Topic, Payload: frame.Payload}:
			case <-c.closed:
				return
			}
		default:
			c.log.Printf("unknown broker frame kind %q", frame.Kind)
		}
	}
}

// deliverAck wakes the waiter registered for sequence, if any. An ack for a
// sequence nobody is waiting on (the waiter gave up on ctx) is ignored.
func deliverAck(c *Client, sequence uint64) {
	c.pendingMu.Lock()
	ack, ok := c.pending[sequence]
	if ok {
		delete(c.pending, sequence)
	}
	c.pendingMu.Unlock()
	if ok {
		close(ack)
	}
}

// isExpectedReadError reports whether err is one of the normal
// terminations (peer closed, local close) for the client reader.
func isExpectedReadError(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}
