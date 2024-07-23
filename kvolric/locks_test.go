package kvolric_test

import (
	"testing"
	"time"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvolric"
	"github.com/royalcat/kv/testsuite"
)

func TestLocks(t *testing.T) {

	testsuite.GoldenLocks(t, func() (kv.Locks[string], error) {
		db, err := newDB()
		if err != nil {
			return nil, err
		}

		dm, err := db.NewEmbeddedClient().NewDMap("test")
		if err != nil {
			return nil, err
		}

		return kvolric.NewLocks(dm, time.Minute), nil
	})
}
