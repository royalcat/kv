package kvbadger

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

func NewBadgerKVBytes[K, V kv.Bytes](opts Options) (kv.Store[K, V], error) {
	db, err := badger.Open(opts.BadgerOptions)
	if err != nil {
		return nil, err
	}
	return &storeBytes[K, V]{badgerStore: badgerStore{
		DB:      db,
		Options: opts,
	}}, nil
}

type storeBytes[K, V kv.Bytes] struct {
	badgerStore
}

func (s *storeBytes[K, V]) Set(ctx context.Context, k K, v V) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		data, err := s.Codec.Marshal(v)
		if err != nil {
			return err
		}
		return txn.Set([]byte(k), data)
	})
}

func (s *storeBytes[K, V]) Get(ctx context.Context, k K) (v V, found bool, err error) {
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
		err = s.Codec.Unmarshal(data, v)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return v, found, err
}

func (s *storeBytes[K, V]) Delete(ctx context.Context, k K) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(k))
	})
}

func (s *storeBytes[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, prefixOptions([]byte(k)), iter)
}

func (s *storeBytes[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, badger.DefaultIteratorOptions, iter)
}

func (s *storeBytes[K, V]) RangeWithOptions(ctx context.Context, opt badger.IteratorOptions, iter kv.Iter[K, V]) error {
	return s.rawRange(ctx, opt, func(k, v []byte) error {
		return iter(K(k), V(v))
	})
}

var _ kv.TransactionalStore[string, string] = (*storeBytes[string, string])(nil)

func (s *storeBytes[K, V]) Transaction(update bool) (kv.Store[K, V], error) {
	tx := s.DB.NewTransaction(update)
	return &transactionBytes[K, V]{
		tx: tx,
	}, nil
}

type transactionBytes[K, V kv.Bytes] struct {
	tx *badger.Txn
}

var _ kv.Store[string, string] = (*transactionBytes[string, string])(nil)

func (t *transactionBytes[K, V]) Close(ctx context.Context) error {
	return t.tx.Commit()
}

// Delete implements kv.Store.
func (t *transactionBytes[K, V]) Delete(ctx context.Context, k K) error {
	return t.tx.Delete([]byte(k))
}

// Get implements kv.Store.
func (t *transactionBytes[K, V]) Get(ctx context.Context, k K) (V, bool, error) {
	v, found, err := txGet[[]byte](t.tx, []byte(k), noopCodec)
	if v == nil || found != true || err != nil {
		var vv V
		return vv, found, err
	}

	return V(v), found, err
}

// Set implements kv.Store.
func (t *transactionBytes[K, V]) Set(ctx context.Context, k K, v V) error {
	return txSet[[]byte](t.tx, []byte(k), []byte(v), noopCodec)

}

// Range implements kv.Store.
func (t *transactionBytes[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return txRange[[]byte](ctx, t.tx, badger.DefaultIteratorOptions, noopCodec, func(k []byte, v []byte) error {
		return iter(K(k), V(v))
	})
}

// RangeWithPrefix implements kv.Store.
func (t *transactionBytes[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return txRange[[]byte](ctx, t.tx, prefixOptions([]byte(k)), noopCodec, func(k []byte, v []byte) error {
		return iter(K(k), V(v))
	})
}
