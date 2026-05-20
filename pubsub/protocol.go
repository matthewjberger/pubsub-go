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
//	{"kind":"subscribe","id":"weather-sub","topic":"weather/current"}
//	{"kind":"publish","id":"weather-pub","topic":"weather/current","payload":{"temp_c":21.4}}
//	{"kind":"unsubscribe","id":"weather-sub","topic":"weather/current"}
//	{"kind":"ping"}
//
// [PeerEvent.Payload] is held as raw JSON so the broker never has to know
// the shape of an application payload.
type PeerEvent struct {
	Kind    PeerEventKind   `json:"kind"`
	ID      string          `json:"id,omitempty"`
	Topic   string          `json:"topic,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// BrokerMessage is the only message type the broker sends to a subscriber.
// [BrokerMessage.Payload] is the exact raw JSON the publisher sent — the
// broker does not re-encode it.
//
//	{"topic":"weather/current","payload":{"temp_c":21.4}}
type BrokerMessage struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}
