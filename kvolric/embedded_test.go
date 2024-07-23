package kvolric_test

import (
	"log"
	"math/rand"
	"testing"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvolric"
	"github.com/royalcat/kv/testsuite"
)

func newStore() (kv.Store[string, string], error) {
	c := config.New("local")
	c.BindPort = 10000 + rand.Int()%10000
	c.MemberlistConfig.BindPort = 10000 + rand.Int()%10000
	// c.BindPort = 10000 + rand.Int()%40000

	// Callback function. It's called when this node is ready to accept connections.
	started := make(chan struct{})
	c.Started = func() {
		close(started)
	}

	db, err := olric.New(c)
	if err != nil {
		return nil, err
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

	opts := kvolric.DefaultOptions[string]()
	opts.Codec = kv.CodecBytes[string]{}
	return kvolric.NewEmbedded(db, "test", opts)
}

func TestEmbeddedGolden(t *testing.T) {
	testsuite.GoldenStrings(t, newStore)
}
