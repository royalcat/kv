package kvbadger

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

func NewBadgerKVBytesKey[K kv.Bytes, V any](opts Options[V]) (kv.Store[K, V], error) {
	db, err := badger.Open(opts.BadgerOptions)
	if err != nil {
		return nil, err
	}

	return &storeBytesKey[K, V]{badgerStore: badgerStore[V]{
		DB:      db,
		Options: opts,
	}}, nil
}

type storeBytesKey[K kv.Bytes, V any] struct {
	badgerStore[V]
}

func (s *storeBytesKey[K, V]) Set(ctx context.Context, k K, v V) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txSet(txn, []byte(k), v, s.Options)
	})
}

func (s *storeBytesKey[K, V]) Get(ctx context.Context, k K) (v V, err error) {
	err = s.DB.View(func(txn *badger.Txn) error {
		v, err = txGet[V](txn, []byte(k), s.Options)
		return err
	})
	return v, err
}

// Get implements Store.
func (s *storeBytesKey[K, V]) Edit(ctx context.Context, k K, edit kv.Edit[V]) error {
	kb := []byte(k)

	return s.DB.Update(func(txn *badger.Txn) error {
		v, err := txGet[V](txn, kb, s.Options)
		if err != nil {
			return err
		}
		v, err = edit(ctx, v)
		if err != nil {
			return err
		}
		return txSet[V](txn, kb, v, s.Options)
	})
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

	s.DB.View(func(txn *badger.Txn) error {
		return txRange(ctx, txn, opts, s.Options, func(k []byte, v V) error {
			return iter(K(k), v)
		})
	})

	return nil
}

func (s *storeBytesKey[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, badger.DefaultIteratorOptions, iter)
}

func (s *storeBytesKey[K, V]) RangeWithOptions(ctx context.Context, opt badger.IteratorOptions, iter kv.Iter[K, V]) error {
	return s.DB.View(func(txn *badger.Txn) error {
		return txRange(ctx, txn, opt, s.Options, func(k []byte, v V) error {
			return iter(K(k), v)
		})
	})
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
	badgerStore[V]
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
func (t *transactionBytesKey[K, V]) Get(ctx context.Context, k K) (V, error) {
	return txGet[V](t.tx, []byte(k), t.Options)
}

// Get implements Store.
func (s *transactionBytesKey[K, V]) Edit(ctx context.Context, k K, edit kv.Edit[V]) error {
	kb := []byte(k)

	v, err := txGet[V](s.tx, kb, s.Options)
	if err != nil {
		return err
	}

	v, err = edit(ctx, v)
	if err != nil {
		return err
	}

	return txSet[V](s.tx, kb, v, s.Options)
}

// Set implements kv.Store.
func (t *transactionBytesKey[K, V]) Set(ctx context.Context, k K, v V) error {
	return txSet(t.tx, []byte(k), v, t.Options)
}

// Range implements kv.Store.
func (t *transactionBytesKey[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return txRange(ctx, t.tx, badger.DefaultIteratorOptions, t.Options, func(k []byte, v V) error {
		return iter(K(k), v)
	})
}

// RangeWithPrefix implements kv.Store.
func (t *transactionBytesKey[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return txRange(ctx, t.tx, prefixOptions([]byte(k)), t.Options, func(k []byte, v V) error {
		return iter(K(k), v)
	})
}
