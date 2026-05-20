// Command publisher connects to a [pubsub] broker and publishes a synthetic
// reading on a topic at a fixed interval. Used together with
// cmd/subscriber to exercise the end-to-end protocol.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/matthewjberger/pubsub-go/pubsub"
)

// Weather is the demo payload type. It is plain data with JSON tags —
// nothing protocol-specific lives on it.
type Weather struct {
	TempCelsius float64 `json:"temp_c"`
	Humidity    float64 `json:"humidity"`
	Tick        int     `json:"tick"`
}

func main() {
	address := flag.String("address", "127.0.0.1:9000", "broker host:port")
	topic := flag.String("topic", "weather/current", "topic to publish on")
	id := flag.String("id", "weather-publisher", "client ID to register with the broker")
	interval := flag.Duration("interval", 500*time.Millisecond, "publish interval")
	flag.Parse()

	client, err := pubsub.ConnectClient(pubsub.ClientConfig{ID: *id, Address: *address})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	tick := 0
	for {
		select {
		case <-ticker.C:
			tick++
			payload := Weather{
				TempCelsius: 20 + 5*sinTick(tick),
				Humidity:    60 + 10*cosTick(tick),
				Tick:        tick,
			}
			if err := pubsub.Publish(client, *topic, payload); err != nil {
				log.Printf("publish: %v", err)
				return
			}
			log.Printf("published %s -> %+v", *topic, payload)
		case <-signals:
			return
		}
	}
}

// sinTick / cosTick avoid pulling in math just so the demo wiggles a bit.
// Returns a saw-tooth-ish value in roughly [-1, 1] keyed off tick.
func sinTick(tick int) float64 {
	cycle := tick % 20
	if cycle < 10 {
		return float64(cycle)/10 - 0.5
	}
	return float64(20-cycle)/10 - 0.5
}

func cosTick(tick int) float64 {
	return sinTick(tick + 5)
}
