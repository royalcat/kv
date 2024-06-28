package kvbadger

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

func prefixOptions(prefix []byte) badger.IteratorOptions {
	return badger.IteratorOptions{
		PrefetchSize:   badger.DefaultIteratorOptions.PrefetchSize,
		PrefetchValues: badger.DefaultIteratorOptions.PrefetchValues,
		Reverse:        badger.DefaultIteratorOptions.Reverse,
		AllVersions:    badger.DefaultIteratorOptions.AllVersions,
		InternalAccess: badger.DefaultIteratorOptions.InternalAccess,
		SinceTs:        badger.DefaultIteratorOptions.SinceTs,
		Prefix:         prefix,
	}
}

type BadgerStore interface {
	BadgerDB() *badger.DB
}

type badgerStore[V any] struct {
	DB      *badger.DB
	Options Options[V]
}

func (s *badgerStore[V]) Close(ctx context.Context) error {
	return s.DB.Close()
}

func (s *badgerStore[V]) BadgerDB() *badger.DB {
	return s.DB
}
