package kvbadger

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

func NewRaw[K, V kv.Bytes](opts Options[V]) (*StoreRaw[K, V], error) {
	db, err := badger.Open(opts.BadgerOptions)
	if err != nil {
		return nil, err
	}
	opts.Codec = kv.CodecBytes[V]{}
	return &StoreRaw[K, V]{badgerStore: badgerStore[V]{
		DB:      db,
		Options: opts,
	}}, nil
}

type StoreRaw[K, V kv.Bytes] struct {
	badgerStore[V]
}

// Get implements Store.
func (s *StoreRaw[K, V]) Edit(ctx context.Context, k K, edit kv.Edit[V]) error {
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

func (s *StoreRaw[K, V]) Set(ctx context.Context, k K, v V) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txSet[V](txn, []byte(k), V(v), s.Options)
	})
}

func (s *StoreRaw[K, V]) Get(ctx context.Context, k K) (v V, err error) {
	err = s.DB.View(func(txn *badger.Txn) error {
		v, err = txGet[V](txn, []byte(k), s.Options)
		return err
	})
	return v, err
}

func (s *StoreRaw[K, V]) Delete(ctx context.Context, k K) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(k))
	})
}

func (s *StoreRaw[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, prefixOptions([]byte(k)), iter)
}

func (s *StoreRaw[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, badger.DefaultIteratorOptions, iter)
}

func (s *StoreRaw[K, V]) RangeWithOptions(ctx context.Context, opt badger.IteratorOptions, iter kv.Iter[K, V]) error {
	return s.DB.View(func(txn *badger.Txn) error {
		return txRange[V](ctx, txn, opt, s.Options, func(k []byte, v V) error {
			return iter(K(k), v)
		})
	})
}

var _ kv.TransactionalStore[string, string] = (*StoreRaw[string, string])(nil)

func (s *StoreRaw[K, V]) Transaction(update bool) (kv.Store[K, V], error) {
	tx := s.DB.NewTransaction(update)
	return &transactionBytes[K, V]{
		tx: tx,
	}, nil
}

type transactionBytes[K, V kv.Bytes] struct {
	tx  *badger.Txn
	opt Options[V]
}

var _ kv.Store[string, string] = (*transactionBytes[string, string])(nil)

func (t *transactionBytes[K, V]) Close(ctx context.Context) error {
	return t.tx.Commit()
}

// Delete implements kv.Store.
func (t *transactionBytes[K, V]) Delete(ctx context.Context, k K) error {
	return t.tx.Delete([]byte(k))
}

// Get implements Store.
func (s *transactionBytes[K, V]) Edit(ctx context.Context, k K, edit kv.Edit[V]) error {
	kb := []byte(k)

	v, err := txGet[V](s.tx, kb, s.opt)
	if err != nil {
		return err
	}

	v, err = edit(ctx, v)
	if err != nil {
		return err
	}

	return txSet[V](s.tx, kb, v, s.opt)
}

// Get implements kv.Store.
func (t *transactionBytes[K, V]) Get(ctx context.Context, k K) (V, error) {
	v, err := txGet[V](t.tx, []byte(k), t.opt)
	return V(v), err
}

// Set implements kv.Store.
func (t *transactionBytes[K, V]) Set(ctx context.Context, k K, v V) error {
	return txSet[V](t.tx, []byte(k), V(v), t.opt)

}

// Range implements kv.Store.
func (t *transactionBytes[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return txRange[V](ctx, t.tx, badger.DefaultIteratorOptions, t.opt, func(k []byte, v V) error {
		return iter(K(k), V(v))
	})
}

// RangeWithPrefix implements kv.Store.
func (t *transactionBytes[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return txRange[V](ctx, t.tx, prefixOptions([]byte(k)), t.opt, func(k []byte, v V) error {
		return iter(K(k), V(v))
	})
}
