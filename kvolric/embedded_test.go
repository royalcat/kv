package kvolric_test

import (
	"log"
	"testing"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
	"github.com/royalcat/kv/kvolric"
)

func TestEmbeddedGolder(t *testing.T) {
	c := config.New("local")

	// Callback function. It's called when this node is ready to accept connections.
	started := make(chan struct{})
	c.Started = func() {
		close(started)
	}

	db, err := olric.New(c)
	if err != nil {
		log.Fatalf("Failed to create Olric instance: %v", err)
	}

	// Start the instance. It will form a single-node cluster.
	go func() {
		// Call Start at background. It's a blocker call.
		err = db.Start()
		if err != nil {
			log.Fatalf("olric.Start returned an error: %v", err)
		}
	}()

	<-started

	kvolric.NewEmbedded[string](db, "test")
}
