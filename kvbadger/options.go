package kvbadger

import (
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

type Options[V any] struct {
	Codec         kv.Codec[V]
	BadgerOptions badger.Options
	DefaultTTL    time.Duration
}

func DefaultOptions[V any](dir string) Options[V] {
	return Options[V]{
		Codec:         kv.JSONCodec[V]{},
		BadgerOptions: badger.DefaultOptions(dir),
	}
}
