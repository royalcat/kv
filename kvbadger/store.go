package kvbadger

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
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

type badgerStore struct {
	DB *badger.DB
	Options
}

func (s *badgerStore) Close(ctx context.Context) error {
	return s.DB.Close()
}

func (s *badgerStore) rawRange(_ context.Context, opt badger.IteratorOptions, iter kv.Iter[[]byte, []byte]) error {
	return s.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			data, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			iter(item.KeyCopy(nil), data)
		}

		return nil
	})
}

func (s *badgerStore) BadgerDB() *badger.DB {
	return s.DB
}
