package kvbadger

import (
	"bytes"
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

func NewBadgerKVBytesKey[K kv.Bytes, V any](opts Options) (kv.Store[K, V], error) {
	db, err := badger.Open(opts.BadgerOptions)
	if err != nil {
		return nil, err
	}

	return &storeBytesKey[K, V]{badgerStore: badgerStore{
		DB:      db,
		Options: opts,
	}}, nil
}

type storeBytesKey[K kv.Bytes, V any] struct {
	badgerStore
}

func (s *storeBytesKey[K, V]) Set(ctx context.Context, k K, v V) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		data, err := s.Codec.Marshal(v)
		if err != nil {
			return err
		}
		return txn.Set([]byte(k), data)
	})
}

func (s *storeBytesKey[K, V]) Get(ctx context.Context, k K) (v V, found bool, err error) {
	err = s.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(k))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				found = false
				return nil
			}
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		err = s.Codec.Unmarshal(data, &v)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return v, found, err
}

func (s *storeBytesKey[K, V]) Delete(ctx context.Context, k K) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(k))
	})
}

func (s *storeBytesKey[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, prefixOptions([]byte(k)), iter)
}

func (s *storeBytesKey[K, V]) RangeOrdered(ctx context.Context, order kv.Order[K], iter kv.Iter[K, V]) error {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = order.Reverse

	data := make([]byte, 0, 32)

	s.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opts)
		for it.Seek([]byte(order.Min)); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if bytes.Compare(k, []byte(order.Max)) == 1 {
				break
			}

			var v V
			err := item.Value(func(val []byte) error {
				return s.Codec.Unmarshal(data, &v)
			})
			if err != nil {
				return err
			}

			return iter(K(k), V(v))
		}
		return nil
	})

	return nil
}

func (s *storeBytesKey[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, badger.DefaultIteratorOptions, iter)
}

func (s *storeBytesKey[K, V]) RangeWithOptions(ctx context.Context, opt badger.IteratorOptions, iter kv.Iter[K, V]) error {
	var err error
	err = s.rawRange(ctx, opt, func(k, val []byte) error {
		var v V
		err = s.Codec.Unmarshal(val, &v)
		if err != nil {
			return err
		}

		return iter(K(k), V(v))
	})

	return err
}

var _ kv.TransactionalStore[string, string] = (*storeBytesKey[string, string])(nil)

// Transaction implements kv.TransactionalStore.
func (s *storeBytesKey[K, V]) Transaction(update bool) (kv.Store[K, V], error) {
	tx := s.DB.NewTransaction(update)
	return &transactionBytesKey[K, V]{
		tx:          tx,
		badgerStore: s.badgerStore,
	}, nil
}

type transactionBytesKey[K kv.Bytes, V any] struct {
	tx *badger.Txn
	badgerStore
}

var _ kv.Store[string, string] = (*transactionBytesKey[string, string])(nil)

func (t *transactionBytesKey[K, V]) Close(ctx context.Context) error {
	return t.tx.Commit()
}

// Delete implements kv.Store.
func (t *transactionBytesKey[K, V]) Delete(ctx context.Context, k K) error {
	return t.tx.Delete([]byte(k))
}

// Get implements kv.Store.
func (t *transactionBytesKey[K, V]) Get(ctx context.Context, k K) (V, bool, error) {
	return txGet[V](t.tx, []byte(k), t.Codec)
}

// Set implements kv.Store.
func (t *transactionBytesKey[K, V]) Set(ctx context.Context, k K, v V) error {
	return txSet(t.tx, []byte(k), v, t.Codec)
}

// Range implements kv.Store.
func (t *transactionBytesKey[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return txRange(ctx, t.tx, badger.DefaultIteratorOptions, t.Codec, func(k []byte, v V) error {
		return iter(K(k), v)
	})
}

// RangeWithPrefix implements kv.Store.
func (t *transactionBytesKey[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return txRange(ctx, t.tx, prefixOptions([]byte(k)), t.Codec, func(k []byte, v V) error {
		return iter(K(k), v)
	})
}
