package pubsub

import "encoding/json"

// PeerEventKind discriminates the variant of a [PeerEvent] on the wire. It
// is a single string field so the JSON form is human-readable and trivial
// to parse from other languages.
type PeerEventKind string

const (
	// PeerEventConnect registers a peer with the broker under [PeerEvent.ID].
	// It must be the first frame a client sends after dialing.
	PeerEventConnect PeerEventKind = "connect"

	// PeerEventPublish asks the broker to fan a payload out to every peer
	// subscribed to [PeerEvent.Topic].
	PeerEventPublish PeerEventKind = "publish"

	// PeerEventSubscribe registers the sender for [PeerEvent.Topic]. Subsequent
	// publishes to that topic will be delivered to this peer until it
	// unsubscribes or disconnects.
	PeerEventSubscribe PeerEventKind = "subscribe"

	// PeerEventUnsubscribe removes the sender from [PeerEvent.Topic].
	PeerEventUnsubscribe PeerEventKind = "unsubscribe"

	// PeerEventPing is a no-op heartbeat. The broker accepts and ignores it.
	PeerEventPing PeerEventKind = "ping"
)

// PeerEvent is the only message type a client sends to the broker. It is a
// flat tagged union: [PeerEvent.Kind] selects which other fields are
// meaningful. Fields that do not apply to a given Kind are zero-valued and
// omitted from JSON.
//
//	{"kind":"connect","id":"weather-pub"}
//	{"kind":"subscribe","topic":"weather/current","seq":1}
//	{"kind":"publish","topic":"weather/current","payload":{"temp_c":21.4}}
//	{"kind":"unsubscribe","topic":"weather/current","seq":2}
//	{"kind":"ping"}
//
// ID is only meaningful on a connect; the broker identifies every later
// frame by the connection it arrived on, not by a field in the frame. Seq
// is set on subscribe and unsubscribe so the broker can echo it back in an
// acknowledgement frame; the client uses that to turn [Subscribe] and
// [Unsubscribe] into synchronous calls. [PeerEvent.Payload]
// is held as raw JSON so the broker never has to know the shape of an
// application payload.
type PeerEvent struct {
	Kind    PeerEventKind   `json:"kind"`
	ID      string          `json:"id,omitempty"`
	Topic   string          `json:"topic,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Seq     uint64          `json:"seq,omitempty"`
}

// BrokerMessage is one delivered publish, as read off the channel returned
// by [Inbox]. [BrokerMessage.Payload] is the exact raw JSON the publisher
// sent; the broker does not re-encode it.
//
//	{"topic":"weather/current","payload":{"temp_c":21.4}}
//
// It is the decoded application-facing half of a message-kind [brokerFrame];
// acknowledgement frames never reach the inbox.
type BrokerMessage struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

// brokerFrameKind discriminates the variant of a [brokerFrame] on the wire.
type brokerFrameKind string

const (
	// brokerFrameMessage carries a published payload to a subscriber.
	brokerFrameMessage brokerFrameKind = "message"

	// brokerFrameAck confirms that the broker applied a subscribe or
	// unsubscribe. It echoes the [PeerEvent.Seq] of the request.
	brokerFrameAck brokerFrameKind = "ack"
)

// brokerFrame is the only message type the broker sends to a client. Like
// [PeerEvent] it is a flat tagged union keyed on Kind.
//
//	{"kind":"message","topic":"weather/current","payload":{"temp_c":21.4}}
//	{"kind":"ack","seq":1}
//
// The type is unexported because Go callers consume decoded [BrokerMessage]
// values from [Inbox] and never handle acknowledgements directly; the wire
// shape is documented in docs/PROTOCOL.md for non-Go peers. The fields are
// exported so encoding/json can see them.
type brokerFrame struct {
	Kind    brokerFrameKind `json:"kind"`
	Topic   string          `json:"topic,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Seq     uint64          `json:"seq,omitempty"`
}
