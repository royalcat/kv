package kvbadger

import (
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

type Options struct {
	Codec         kv.Codec
	BadgerOptions badger.Options
	DefaultTTL    time.Duration
}

func DefaultOptions(dir string) Options {
	return Options{
		Codec:         kv.JSONCodec{},
		BadgerOptions: badger.DefaultOptions(dir),
	}
}
