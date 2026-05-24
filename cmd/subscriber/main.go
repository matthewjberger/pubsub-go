// Command subscriber connects to a [pubsub] broker, subscribes to one or
// more topics, and prints every message it receives. Used together with
// cmd/publisher to exercise the end-to-end protocol.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/matthewjberger/pubsub-go/pubsub"
)

func main() {
	address := flag.String("address", "127.0.0.1:9000", "broker host:port")
	topicsFlag := flag.String("topics", "weather/current", "comma-separated list of topics to subscribe to")
	id := flag.String("id", "subscriber", "client ID to register with the broker")
	flag.Parse()

	topics := splitTopics(*topicsFlag)
	if len(topics) == 0 {
		log.Fatalf("at least one topic is required")
	}

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := pubsub.ConnectClient(connectCtx, pubsub.ClientConfig{
		ID:            *id,
		Address:       *address,
		InboxCapacity: 64,
	})
	cancelConnect()
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	for _, topic := range topics {
		subscribeCtx, cancelSubscribe := context.WithTimeout(context.Background(), 5*time.Second)
		err := pubsub.Subscribe(subscribeCtx, client, topic)
		cancelSubscribe()
		if err != nil {
			log.Fatalf("subscribe %q: %v", topic, err)
		}
		log.Printf("subscribed to %q", topic)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case message, ok := <-pubsub.Inbox(client):
			if !ok {
				log.Printf("inbox closed; exiting")
				return
			}
			log.Printf("recv %s -> %s", message.Topic, string(message.Payload))
		case <-signals:
			return
		}
	}
}

// splitTopics turns a comma-separated topic list into a slice, trimming
// whitespace and dropping empty entries.
func splitTopics(raw string) []string {
	parts := strings.Split(raw, ",")
	out := parts[:0]
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
