package kvbadger

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

type Options struct {
	kv.Options
	BadgerOptions badger.Options
}

func DefaultOptions(dir string) Options {
	return Options{
		Options:       kv.DefaultOptions,
		BadgerOptions: badger.DefaultOptions(dir),
	}
}
