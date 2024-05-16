package kvbadger

import (
	"context"
	"encoding"

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

func NewBadgerKV[K kv.Bytes, V any](opts Options) (kv.Store[K, V], error) {
	db, err := badger.Open(opts.BadgerOptions)
	if err != nil {
		return nil, err
	}

	return &store[K, V]{badgerStore: badgerStore{
		DB:      db,
		Options: opts,
	}}, nil
}

type store[K kv.Bytes, V any] struct {
	badgerStore
}

func (s *store[K, V]) Set(ctx context.Context, k K, v V) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		data, err := s.Codec.Marshal(v)
		if err != nil {
			return err
		}
		return txn.Set([]byte(k), data)
	})
}

func (s *store[K, V]) Get(ctx context.Context, k K) (v V, found bool, err error) {
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

func (s *store[K, V]) Delete(ctx context.Context, k K) error {
	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(k))
	})
}

func (s *store[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, prefixOptions([]byte(k)), iter)
}

func (s *store[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, badger.DefaultIteratorOptions, iter)
}

func (s *store[K, V]) RangeWithOptions(ctx context.Context, opt badger.IteratorOptions, iter kv.Iter[K, V]) error {
	var err error
	err = s.rawRange(ctx, opt, func(k, val []byte) bool {
		var v V
		err = s.Codec.Unmarshal(val, &v)
		if err != nil {
			return false
		}

		return iter(K(k), V(v))
	})

	return err
}

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
	return s.rawRange(ctx, opt, func(k, v []byte) bool {
		return iter(K(k), V(v))
	})
}

func NewBadgerKVMarhsler[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]](opts Options) (kv.Store[K, V], error) {
	db, err := badger.Open(opts.BadgerOptions)
	if err != nil {
		return nil, err
	}
	return &storeInterface[K, V, KP]{badgerStore: badgerStore{
		DB:      db,
		Options: opts,
	}}, nil
}

type storeInterface[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]] struct {
	badgerStore
}

func (s *storeInterface[K, V, KP]) Set(ctx context.Context, k K, v V) error {
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

func (s *storeInterface[K, V, KP]) Get(ctx context.Context, k K) (v V, found bool, err error) {
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

func (s *storeInterface[K, V, KP]) Delete(ctx context.Context, k K) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return s.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete(kb)
	})
}

func (s *storeInterface[K, V, KP]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	p, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return s.RangeWithOptions(ctx, prefixOptions([]byte(p)), iter)
}

func (s *storeInterface[K, V, KP]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return s.RangeWithOptions(ctx, badger.DefaultIteratorOptions, iter)
}

func (s *storeInterface[K, V, KP]) RangeWithOptions(ctx context.Context, opt badger.IteratorOptions, iter kv.Iter[K, V]) error {
	var err error
	err = s.rawRange(ctx, opt, func(key []byte, val []byte) bool {
		var k K
		kp := KP(&k)
		err = kp.UnmarshalBinary(key)
		if err != nil {
			return false
		}

		var v V
		err = s.Codec.Unmarshal(val, &v)
		if err != nil {
			return false
		}

		return iter(*kp, v)
	})
	return err
}

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
