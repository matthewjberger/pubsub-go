package pubsub

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// BrokerConfig configures a [Broker] at startup. Address is a host:port
// string suitable for [net.Listen]. Log is the destination for broker
// diagnostics; if nil, a logger writing to [os.Stderr] is created. Pass
// "127.0.0.1:0" to bind to a kernel-assigned port and read the actual
// address back with [Broker.Address]. WriteTimeout bounds a single socket
// write to one peer; if zero, [DefaultWriteTimeout] is used. A write that
// blocks past the timeout (a stuck or wedged peer) drops that connection so
// its writer goroutine cannot leak. SubscriberBuffer is the per-subscriber
// outbound queue depth; if zero, [DefaultSubscriberBuffer] is used. It is the
// amount a publisher may run ahead of a subscriber before publishing
// backpressures (see [publisher backpressure] in the design docs).
type BrokerConfig struct {
	Address          string
	Log              *log.Logger
	WriteTimeout     time.Duration
	SubscriberBuffer int
}

// DefaultWriteTimeout is the per-write deadline applied to a peer socket
// when [BrokerConfig.WriteTimeout] is left zero.
const DefaultWriteTimeout = 30 * time.Second

// DefaultSubscriberBuffer is the per-subscriber outbound queue depth applied
// when [BrokerConfig.SubscriberBuffer] is left zero.
const DefaultSubscriberBuffer = 256

// Broker is the running state of a pub/sub broker. Construct one with
// [StartBroker]. The map of connected peers and the map of subscriptions
// are owned by a single internal goroutine; the only safe way to mutate
// broker state from outside is through connected [Client]s, or through
// [Broker.Shutdown] to stop the broker.
type Broker struct {
	listener         net.Listener
	log              *log.Logger
	writeTimeout     time.Duration
	subscriberBuffer int
	events           chan brokerEvent
	shutdown         chan struct{}
	shutdownMu       sync.Mutex
	closed           bool
	loopDone         chan struct{}
}

// brokerEventKind tags variants of [brokerEvent].
type brokerEventKind int

const (
	brokerEventConnect brokerEventKind = iota
	brokerEventPeerEvent
	brokerEventPublish
	brokerEventDisconnect
)

// brokerEvent is the broker loop's internal message type. The accept loop
// translates a new connection into a brokerEventConnect, each per-peer
// reader translates subscribe/unsubscribe/ping frames into a
// brokerEventPeerEvent, a publish frame into a brokerEventPublish that asks
// the loop for the topic's current subscribers, and the same reader emits one
// brokerEventDisconnect when the connection drops. The outbound field on
// Connect and Disconnect identifies the specific connection so the loop can
// ignore stale disconnects from an evicted previous peer that shared the same
// ID. reply carries the subscriber snapshot back to a publishing reader.
type brokerEvent struct {
	kind     brokerEventKind
	peerID   string
	conn     net.Conn
	outbound chan []byte
	done     chan struct{}
	event    PeerEvent
	topic    string
	reply    chan []peerHandle
}

// peer is the broker-side view of one connected client. It is touched only
// by the broker loop. done is closed by the loop when the peer is removed or
// evicted; senders and the writer watch it so they stop targeting a peer that
// is gone. outbound is never closed, so a publishing reader can send to it
// without racing a close.
type peer struct {
	outbound chan []byte
	done     chan struct{}
	conn     net.Conn
}

// peerHandle is the subset of a peer a publishing reader needs to deliver a
// frame: the outbound queue to push onto and the done signal to abandon a
// peer that disconnects mid-delivery.
type peerHandle struct {
	outbound chan []byte
	done     chan struct{}
}

// StartBroker binds cfg.Address and starts the accept and broker loops.
// It returns once the listener is up so the caller can publish
// [Broker.Address] before clients dial.
func StartBroker(cfg BrokerConfig) (*Broker, error) {
	logger := cfg.Log
	if logger == nil {
		logger = log.New(os.Stderr, "[broker] ", log.LstdFlags|log.Lmsgprefix)
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout == 0 {
		writeTimeout = DefaultWriteTimeout
	}
	subscriberBuffer := cfg.SubscriberBuffer
	if subscriberBuffer == 0 {
		subscriberBuffer = DefaultSubscriberBuffer
	}
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("pubsub: listen %s: %w", cfg.Address, err)
	}
	b := &Broker{
		listener:         listener,
		log:              logger,
		writeTimeout:     writeTimeout,
		subscriberBuffer: subscriberBuffer,
		events:           make(chan brokerEvent, 256),
		shutdown:         make(chan struct{}),
		loopDone:         make(chan struct{}),
	}
	go runBrokerLoop(b)
	go runAcceptLoop(b)
	logger.Printf("listening on %s", listener.Addr())
	return b, nil
}

// Address returns the actual listening address. Useful when
// [BrokerConfig.Address] requested a kernel-assigned port (":0").
func (b *Broker) Address() string {
	return b.listener.Addr().String()
}

// Shutdown stops accepting new connections, closes every peer connection,
// and waits for the broker loop to drain. Safe to call more than once.
func (b *Broker) Shutdown() error {
	b.shutdownMu.Lock()
	if b.closed {
		b.shutdownMu.Unlock()
		return nil
	}
	b.closed = true
	close(b.shutdown)
	b.shutdownMu.Unlock()

	if err := b.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		b.log.Printf("close listener: %v", err)
	}
	<-b.loopDone
	return nil
}

// runAcceptLoop accepts incoming TCP connections and spawns a reader
// goroutine per connection.
func runAcceptLoop(b *Broker) {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.shutdown:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			b.log.Printf("accept: %v", err)
			continue
		}
		go runConnectionReader(b, conn)
	}
}

// runConnectionReader reads PeerEvent frames from one TCP connection and
// drives them. The first frame must be a PeerEventConnect; anything else
// closes the connection. Subscribe, unsubscribe, and ping frames are handed
// to the broker loop; a publish frame is fanned out here, on this goroutine,
// so a slow subscriber backpressures this connection (and through it the
// publishing client) rather than being dropped. On exit it always emits one
// brokerEventDisconnect carrying its outbound channel so the broker loop can
// tell this peer's disconnect apart from any later peer that registers under
// the same ID.
func runConnectionReader(b *Broker, conn net.Conn) {
	reader := bufio.NewReader(conn)
	var firstEvent PeerEvent
	if err := ReadFrame(reader, &firstEvent); err != nil {
		b.log.Printf("read handshake from %s: %v", conn.RemoteAddr(), err)
		_ = conn.Close()
		return
	}
	if firstEvent.Kind != PeerEventConnect || firstEvent.ID == "" {
		b.log.Printf("first frame from %s was not a connect: %+v", conn.RemoteAddr(), firstEvent)
		_ = conn.Close()
		return
	}
	outbound := make(chan []byte, b.subscriberBuffer)
	done := make(chan struct{})
	id := firstEvent.ID

	select {
	case b.events <- brokerEvent{
		kind:     brokerEventConnect,
		peerID:   id,
		conn:     conn,
		outbound: outbound,
		done:     done,
	}:
	case <-b.shutdown:
		_ = conn.Close()
		return
	}

	go runConnectionWriter(b, conn, id, outbound, done)

	for {
		var next PeerEvent
		if err := ReadFrame(reader, &next); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				b.log.Printf("read from %s (%s): %v", id, conn.RemoteAddr(), err)
			}
			break
		}
		if !handleInboundEvent(b, id, next) {
			break
		}
	}

	_ = conn.Close()
	select {
	case b.events <- brokerEvent{kind: brokerEventDisconnect, peerID: id, outbound: outbound}:
	case <-b.shutdown:
	}
}

// handleInboundEvent processes one frame read from a peer. A publish is
// delivered inline (see deliverPublish); everything else is handed to the
// broker loop. It returns false when the broker is shutting down, which tells
// the read loop to stop.
func handleInboundEvent(b *Broker, id string, event PeerEvent) bool {
	if event.Kind == PeerEventPublish {
		return deliverPublish(b, id, event)
	}
	select {
	case b.events <- brokerEvent{kind: brokerEventPeerEvent, peerID: id, event: event}:
		return true
	case <-b.shutdown:
		return false
	}
}

// deliverPublish fans one publish out to every current subscriber of its
// topic. It encodes the message frame once, asks the broker loop for the
// topic's subscriber snapshot, then sends the shared bytes to each
// subscriber's outbound queue. A send blocks while a subscriber's queue is
// full, which stalls this reader, stops it draining the publisher's socket,
// and so backpressures the publishing client to the slowest subscriber's
// rate. A subscriber that disconnects mid-delivery is abandoned via its done
// signal rather than blocking forever. Returns false on broker shutdown.
func deliverPublish(b *Broker, id string, event PeerEvent) bool {
	frame, err := encodeFrame(brokerFrame{Kind: brokerFrameMessage, Topic: event.Topic, Payload: event.Payload})
	if err != nil {
		b.log.Printf("encode publish from %q on %q: %v", id, event.Topic, err)
		return true
	}
	reply := make(chan []peerHandle, 1)
	select {
	case b.events <- brokerEvent{kind: brokerEventPublish, peerID: id, topic: event.Topic, reply: reply}:
	case <-b.shutdown:
		return false
	}
	var targets []peerHandle
	select {
	case targets = <-reply:
	case <-b.shutdown:
		return false
	}
	for _, target := range targets {
		select {
		case target.outbound <- frame:
		case <-target.done:
		case <-b.shutdown:
			return false
		}
	}
	return true
}

// runConnectionWriter drains outbound onto conn until the connection closes,
// the peer is removed, or the broker shuts down. Each frame is pre-encoded by
// the broker loop or a publishing reader, so the writer never marshals.
func runConnectionWriter(b *Broker, conn net.Conn, id string, outbound chan []byte, done chan struct{}) {
	for {
		select {
		case frame := <-outbound:
			if b.writeTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(b.writeTimeout))
			}
			if _, err := conn.Write(frame); err != nil {
				b.log.Printf("write to %s (%s): %v", id, conn.RemoteAddr(), err)
				_ = conn.Close()
				return
			}
		case <-done:
			return
		case <-b.shutdown:
			_ = conn.Close()
			return
		}
	}
}

// runBrokerLoop is the single mutator of the peer and subscription maps.
// All state transitions happen here, on this one goroutine.
func runBrokerLoop(b *Broker) {
	defer close(b.loopDone)
	peers := map[string]peer{}
	subscriptions := map[string]map[string]struct{}{}

	for {
		select {
		case <-b.shutdown:
			for _, p := range peers {
				_ = p.conn.Close()
			}
			return
		case event := <-b.events:
			handleBrokerEvent(b, peers, subscriptions, event)
		}
	}
}

// handleBrokerEvent dispatches one broker event to the right per-kind
// handler.
func handleBrokerEvent(b *Broker, peers map[string]peer, subscriptions map[string]map[string]struct{}, event brokerEvent) {
	switch event.kind {
	case brokerEventConnect:
		registerPeer(b, peers, event)
	case brokerEventPeerEvent:
		handlePeerEvent(b, peers, subscriptions, event.peerID, event.event)
	case brokerEventPublish:
		event.reply <- collectSubscribers(peers, subscriptions, event.topic)
	case brokerEventDisconnect:
		removePeerIfCurrent(b, peers, subscriptions, event.peerID, event.outbound)
	}
}

// registerPeer wires a freshly-connected peer into the broker. If a peer
// with the same ID is already registered, its connection is closed and its
// done signal fired so its writer stops and any reader delivering to it
// abandons it; the most-recent connection wins.
func registerPeer(b *Broker, peers map[string]peer, event brokerEvent) {
	if existing, ok := peers[event.peerID]; ok {
		b.log.Printf("peer %q reconnecting; closing previous connection", event.peerID)
		_ = existing.conn.Close()
		close(existing.done)
	}
	peers[event.peerID] = peer{
		outbound: event.outbound,
		done:     event.done,
		conn:     event.conn,
	}
	b.log.Printf("peer connected: %q (%s)", event.peerID, event.conn.RemoteAddr())
}

// handlePeerEvent dispatches one PeerEvent received from peerID. Publishes do
// not arrive here; they are fanned out on the reader goroutine via
// deliverPublish so a slow subscriber backpressures the publisher.
func handlePeerEvent(b *Broker, peers map[string]peer, subscriptions map[string]map[string]struct{}, peerID string, event PeerEvent) {
	switch event.Kind {
	case PeerEventSubscribe:
		addSubscription(b, subscriptions, peerID, event.Topic)
		acknowledge(b, peers, peerID, event.Seq)
	case PeerEventUnsubscribe:
		removeSubscription(b, subscriptions, peerID, event.Topic)
		acknowledge(b, peers, peerID, event.Seq)
	case PeerEventPing:
	case PeerEventConnect:
		b.log.Printf("ignoring duplicate connect from %q", peerID)
	default:
		b.log.Printf("unknown event kind %q from %q", event.Kind, peerID)
	}
}

// collectSubscribers snapshots the handles of every peer currently subscribed
// to topic. The publishing reader delivers to these without holding the
// broker loop. A peer that subscribes after this snapshot does not receive
// the in-flight publish, which is the same ordering any pub/sub has.
func collectSubscribers(peers map[string]peer, subscriptions map[string]map[string]struct{}, topic string) []peerHandle {
	subscribers, ok := subscriptions[topic]
	if !ok || len(subscribers) == 0 {
		return nil
	}
	handles := make([]peerHandle, 0, len(subscribers))
	for subscriberID := range subscribers {
		if p, ok := peers[subscriberID]; ok {
			handles = append(handles, peerHandle{outbound: p.outbound, done: p.done})
		}
	}
	return handles
}

// acknowledge sends an ack frame echoing seq back to peerID so a synchronous
// [Subscribe] or [Unsubscribe] can return. A zero seq (a peer that did not
// ask for an ack) is skipped. The ack send is non-blocking so it cannot stall
// the broker loop behind a slow peer: if the peer's outbound queue is full the
// ack is dropped and the caller's context deadline eventually fires. Subscribe
// and unsubscribe are idempotent, so a retry after a dropped ack is safe.
func acknowledge(b *Broker, peers map[string]peer, peerID string, seq uint64) {
	if seq == 0 {
		return
	}
	p, ok := peers[peerID]
	if !ok {
		return
	}
	frame, err := encodeFrame(brokerFrame{Kind: brokerFrameAck, Seq: seq})
	if err != nil {
		b.log.Printf("encode ack seq %d for %q: %v", seq, peerID, err)
		return
	}
	select {
	case p.outbound <- frame:
	default:
		b.log.Printf("dropping ack seq %d for slow peer %q", seq, peerID)
	}
}

// addSubscription registers peerID as a subscriber of topic. Idempotent.
func addSubscription(b *Broker, subscriptions map[string]map[string]struct{}, peerID, topic string) {
	subscribers, ok := subscriptions[topic]
	if !ok {
		subscribers = map[string]struct{}{}
		subscriptions[topic] = subscribers
	}
	subscribers[peerID] = struct{}{}
	b.log.Printf("peer %q subscribed to %q", peerID, topic)
}

// removeSubscription removes peerID from topic. Drops the topic entry
// entirely when the last subscriber leaves so empty maps don't pile up.
func removeSubscription(b *Broker, subscriptions map[string]map[string]struct{}, peerID, topic string) {
	subscribers, ok := subscriptions[topic]
	if !ok {
		return
	}
	delete(subscribers, peerID)
	if len(subscribers) == 0 {
		delete(subscriptions, topic)
	}
	b.log.Printf("peer %q unsubscribed from %q", peerID, topic)
}

// removePeerIfCurrent evicts a peer and scrubs it from every topic, but
// only if the peer currently registered under peerID is the same one that
// owned outbound. A previous peer evicted by registerPeer eventually
// reports a disconnect of its own, which we drop silently so they can't
// remove the new peer.
func removePeerIfCurrent(b *Broker, peers map[string]peer, subscriptions map[string]map[string]struct{}, peerID string, outbound chan []byte) {
	current, ok := peers[peerID]
	if !ok || current.outbound != outbound {
		return
	}
	delete(peers, peerID)
	close(current.done)
	for topic, subscribers := range subscriptions {
		if _, ok := subscribers[peerID]; ok {
			delete(subscribers, peerID)
			if len(subscribers) == 0 {
				delete(subscriptions, topic)
			}
		}
	}
	b.log.Printf("peer disconnected: %q", peerID)
}
