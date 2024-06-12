package kvbadger

import (
	"context"
	"encoding"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

func NewBagerKVBinaryKey[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]](opts Options) (kv.Store[K, V], error) {
	db, err := badger.Open(opts.BadgerOptions)
	if err != nil {
		return nil, err
	}
	return &storeBinaryKey[K, V, KP]{badgerStore: badgerStore{
		DB:      db,
		Options: opts,
	}}, nil
}

type storeBinaryKey[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]] struct {
	badgerStore
}

func (s *storeBinaryKey[K, V, KP]) Set(ctx context.Context, k K, v V) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return s.DB.Update(func(txn *badger.Txn) error {
		return txSet(txn, kb, v, s.Options)
	})
}

func (s *storeBinaryKey[K, V, KP]) Get(ctx context.Context, k K) (v V, found bool, err error) {
	kb, err := k.MarshalBinary()
	if err != nil {
		return v, found, err
	}

	err = s.DB.View(func(txn *badger.Txn) error {
		v, found, err = txGet[V](txn, kb, s.Options)
		return err
	})
	return v, found, err
}

func (s *storeBinaryKey[K, V, KP]) Delete(ctx context.Context, k K) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete(kb)
	})
}

func (s *storeBinaryKey[K, V, KP]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	p, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return s.RangeWithOptions(ctx, prefixOptions([]byte(p)), iter)
}

func (s *storeBinaryKey[K, V, KP]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, badger.DefaultIteratorOptions, iter)
}

func (s *storeBinaryKey[K, V, KP]) RangeWithOptions(ctx context.Context, opt badger.IteratorOptions, iter kv.Iter[K, V]) error {
	return s.DB.View(func(txn *badger.Txn) error {
		return txRange[V](ctx, txn, opt, s.Options, func(k []byte, v V) error {
			var key K
			kp := KP(&key)
			err := kp.UnmarshalBinary(k)
			if err != nil {
				return err
			}

			return iter(key, v)
		})
	})
}

func (s *storeBinaryKey[K, V, KP]) Transaction(update bool) (kv.Store[K, V], error) {
	tx := s.DB.NewTransaction(update)
	return &transactionBinaryKey[K, V, KP]{
		tx: tx,
	}, nil
}

type transactionBinaryKey[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]] struct {
	tx *badger.Txn
	badgerStore
}

func (t *transactionBinaryKey[K, V, KP]) Close(ctx context.Context) error {
	return t.tx.Commit()
}

// Delete implements kv.Store.
func (t *transactionBinaryKey[K, V, KP]) Delete(ctx context.Context, k K) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}
	return t.tx.Delete(kb)
}

// Get implements kv.Store.
func (t *transactionBinaryKey[K, V, KP]) Get(ctx context.Context, k K) (V, bool, error) {
	kb, err := k.MarshalBinary()
	if err != nil {
		var v V
		return v, false, err
	}

	return txGet[V](t.tx, kb, t.Options)
}

// Set implements kv.Store.
func (t *transactionBinaryKey[K, V, KP]) Set(ctx context.Context, k K, v V) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return txSet[V](t.tx, kb, v, t.Options)

}

// Range implements kv.Store.
func (t *transactionBinaryKey[K, V, KP]) Range(ctx context.Context, iter kv.Iter[K, V]) error {

	var err error
	return txRange(ctx, t.tx, badger.DefaultIteratorOptions, t.Options, func(kb []byte, v V) error {
		var k K
		kp := KP(&k)
		err = kp.UnmarshalBinary(kb)
		if err != nil {
			return err
		}

		return iter(k, V(v))
	})
}

// RangeWithPrefix implements kv.Store.
func (t *transactionBinaryKey[K, V, KP]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return txRange[V](ctx, t.tx, prefixOptions(kb), t.Options, func(kb []byte, v V) error {
		var k K
		kp := KP(&k)
		err := kp.UnmarshalBinary(kb)
		if err != nil {
			return err
		}

		return iter(k, V(v))
	})
}
