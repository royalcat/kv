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
		data, err := s.Codec.Marshal(v)
		if err != nil {
			return err
		}
		return txn.Set(kb, data)
	})
}

func (s *storeBinaryKey[K, V, KP]) Get(ctx context.Context, k K) (v V, found bool, err error) {
	kb, err := k.MarshalBinary()
	if err != nil {
		return v, found, err
	}

	err = s.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(kb)
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
	return s.rawRange(ctx, opt, func(key []byte, val []byte) error {
		var k K
		kp := KP(&k)
		err := kp.UnmarshalBinary(key)
		if err != nil {
			return err
		}

		var v V
		err = s.Codec.Unmarshal(val, &v)
		if err != nil {
			return err
		}

		return iter(*kp, v)
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

	return txGet[V](t.tx, kb, t.Codec)
}

// Set implements kv.Store.
func (t *transactionBinaryKey[K, V, KP]) Set(ctx context.Context, k K, v V) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return txSet[V](t.tx, kb, v, t.Codec)

}

// Range implements kv.Store.
func (t *transactionBinaryKey[K, V, KP]) Range(ctx context.Context, iter kv.Iter[K, V]) error {

	var err error
	return txRange(ctx, t.tx, badger.DefaultIteratorOptions, t.Codec, func(kb []byte, v V) error {
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

	return txRange[V](ctx, t.tx, prefixOptions(kb), t.Codec, func(kb []byte, v V) error {
		var k K
		kp := KP(&k)
		err := kp.UnmarshalBinary(kb)
		if err != nil {
			return err
		}

		return iter(k, V(v))
	})
}
