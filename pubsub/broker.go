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
)

// BrokerConfig configures a [Broker] at startup. Address is a host:port
// string suitable for [net.Listen]. Log is the destination for broker
// diagnostics; if nil, a logger writing to [os.Stderr] is created. Pass
// "127.0.0.1:0" to bind to a kernel-assigned port and read the actual
// address back with [Broker.Address].
type BrokerConfig struct {
	Address string
	Log     *log.Logger
}

// Broker is the running state of a pub/sub broker. Construct one with
// [StartBroker]. The map of connected peers and the map of subscriptions
// are owned by a single internal goroutine; the only safe way to mutate
// broker state from outside is through connected [Client]s, or through
// [Broker.Shutdown] to stop the broker.
type Broker struct {
	listener   net.Listener
	log        *log.Logger
	events     chan brokerEvent
	shutdown   chan struct{}
	shutdownMu sync.Mutex
	closed     bool
	loopDone   chan struct{}
}

// brokerEventKind tags variants of [brokerEvent].
type brokerEventKind int

const (
	brokerEventConnect brokerEventKind = iota
	brokerEventPeerEvent
	brokerEventDisconnect
)

// brokerEvent is the broker loop's internal message type. The accept loop
// translates a new connection into a brokerEventConnect, each per-peer
// reader translates wire frames into brokerEventPeerEvent, and the same
// reader emits one brokerEventDisconnect when the connection drops. The
// outbound field on Connect and Disconnect identifies the specific
// connection so the loop can ignore stale disconnects from an evicted
// previous peer that shared the same ID.
type brokerEvent struct {
	kind       brokerEventKind
	peerID     string
	conn       net.Conn
	outbound   chan BrokerMessage
	connClosed chan struct{}
	event      PeerEvent
}

// peer is the broker-side view of one connected client. It is touched only
// by the broker loop.
type peer struct {
	outbound   chan BrokerMessage
	connClosed chan struct{}
	conn       net.Conn
}

// StartBroker binds cfg.Address and starts the accept and broker loops.
// It returns once the listener is up so the caller can publish
// [Broker.Address] before clients dial.
func StartBroker(cfg BrokerConfig) (*Broker, error) {
	logger := cfg.Log
	if logger == nil {
		logger = log.New(os.Stderr, "[broker] ", log.LstdFlags|log.Lmsgprefix)
	}
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("pubsub: listen %s: %w", cfg.Address, err)
	}
	b := &Broker{
		listener: listener,
		log:      logger,
		events:   make(chan brokerEvent, 256),
		shutdown: make(chan struct{}),
		loopDone: make(chan struct{}),
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
// forwards them to the broker loop. The first frame must be a
// PeerEventConnect; anything else closes the connection. On exit it
// always emits one brokerEventDisconnect carrying its outbound channel so
// the broker loop can tell this peer's disconnect apart from any later
// peer that registers under the same ID.
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
	outbound := make(chan BrokerMessage, 64)
	connClosed := make(chan struct{})
	id := firstEvent.ID

	select {
	case b.events <- brokerEvent{
		kind:       brokerEventConnect,
		peerID:     id,
		conn:       conn,
		outbound:   outbound,
		connClosed: connClosed,
	}:
	case <-b.shutdown:
		_ = conn.Close()
		return
	}

	go runConnectionWriter(b, conn, id, outbound, connClosed)

readLoop:
	for {
		var next PeerEvent
		if err := ReadFrame(reader, &next); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				b.log.Printf("read from %s (%s): %v", id, conn.RemoteAddr(), err)
			}
			break readLoop
		}
		select {
		case b.events <- brokerEvent{kind: brokerEventPeerEvent, peerID: id, event: next}:
		case <-b.shutdown:
			break readLoop
		}
	}

	close(connClosed)
	_ = conn.Close()
	select {
	case b.events <- brokerEvent{kind: brokerEventDisconnect, peerID: id, outbound: outbound}:
	case <-b.shutdown:
	}
}

// runConnectionWriter drains outbound onto conn until the connection
// closes or the writer is told to stop.
func runConnectionWriter(b *Broker, conn net.Conn, id string, outbound chan BrokerMessage, connClosed chan struct{}) {
	for {
		select {
		case message, ok := <-outbound:
			if !ok {
				return
			}
			if err := WriteFrame(conn, message); err != nil {
				b.log.Printf("write to %s (%s): %v", id, conn.RemoteAddr(), err)
				_ = conn.Close()
				return
			}
		case <-connClosed:
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
	case brokerEventDisconnect:
		removePeerIfCurrent(b, peers, subscriptions, event.peerID, event.outbound)
	}
}

// registerPeer wires a freshly-connected peer into the broker. If a peer
// with the same ID is already registered, its connection is closed and
// its outbound channel drained; the most-recent connection wins.
func registerPeer(b *Broker, peers map[string]peer, event brokerEvent) {
	if existing, ok := peers[event.peerID]; ok {
		b.log.Printf("peer %q reconnecting; closing previous connection", event.peerID)
		_ = existing.conn.Close()
		close(existing.outbound)
	}
	peers[event.peerID] = peer{
		outbound:   event.outbound,
		connClosed: event.connClosed,
		conn:       event.conn,
	}
	b.log.Printf("peer connected: %q (%s)", event.peerID, event.conn.RemoteAddr())
}

// handlePeerEvent dispatches one PeerEvent received from peerID.
func handlePeerEvent(b *Broker, peers map[string]peer, subscriptions map[string]map[string]struct{}, peerID string, event PeerEvent) {
	switch event.Kind {
	case PeerEventPublish:
		publishToSubscribers(b, peers, subscriptions, event.Topic, event.Payload)
	case PeerEventSubscribe:
		addSubscription(b, subscriptions, peerID, event.Topic)
	case PeerEventUnsubscribe:
		removeSubscription(b, subscriptions, peerID, event.Topic)
	case PeerEventPing:
	case PeerEventConnect:
		b.log.Printf("ignoring duplicate connect from %q", peerID)
	default:
		b.log.Printf("unknown event kind %q from %q", event.Kind, peerID)
	}
}

// publishToSubscribers fans a payload out to every subscriber of topic.
// Delivery is best-effort: if a subscriber's outbound buffer is full, the
// message is dropped for that subscriber and a diagnostic is logged. The
// payload bytes are shared across all recipients; recipients must treat
// them as read-only.
func publishToSubscribers(b *Broker, peers map[string]peer, subscriptions map[string]map[string]struct{}, topic string, payload []byte) {
	subscribers, ok := subscriptions[topic]
	if !ok || len(subscribers) == 0 {
		return
	}
	message := BrokerMessage{Topic: topic, Payload: payload}
	for subscriberID := range subscribers {
		p, ok := peers[subscriberID]
		if !ok {
			continue
		}
		select {
		case p.outbound <- message:
		default:
			b.log.Printf("dropping message on topic %q for slow subscriber %q", topic, subscriberID)
		}
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
// reports a disconnect of its own — we drop those silently so they can't
// remove the new peer.
func removePeerIfCurrent(b *Broker, peers map[string]peer, subscriptions map[string]map[string]struct{}, peerID string, outbound chan BrokerMessage) {
	current, ok := peers[peerID]
	if !ok || current.outbound != outbound {
		return
	}
	delete(peers, peerID)
	close(current.outbound)
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
