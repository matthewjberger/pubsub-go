// Command broker starts a [pubsub] broker on a TCP port and blocks until
// it receives SIGINT or SIGTERM.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/matthewjberger/pubsub-go/pubsub"
)

func main() {
	address := flag.String("address", "127.0.0.1:9000", "host:port to bind")
	flag.Parse()

	broker, err := pubsub.StartBroker(pubsub.BrokerConfig{Address: *address})
	if err != nil {
		log.Fatalf("start broker: %v", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	log.Printf("shutting down")
	if err := broker.Shutdown(); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
