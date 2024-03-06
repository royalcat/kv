package kv

import (
	"encoding"
	"encoding/json"
	"iter"

	"github.com/dgraph-io/badger/v4"
)

func NewBadgerKV[K Bytes, V any](dir string) (Store[K, V], error) {
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}
	return &store[K, V]{db: db}, nil
}

type store[K Bytes, V any] struct {
	db *badger.DB
}

func (s *store[K, V]) Set(k K, v V) error {
	return s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return txn.Set([]byte(k), data)
	})
}

func (s *store[K, V]) Get(k K) (v V, found bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
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
		err = json.Unmarshal(data, &v)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return v, found, err
}

func (s *store[K, V]) Delete(k K) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(k))
	})
}

func (s *store[K, V]) RangeWithPrefix(k K) iter.Seq2[Pair[K, V], error] {
	return s.RangeWithOptions(prefixOptions([]byte(k)))
}

func (s *store[K, V]) Range() iter.Seq2[Pair[K, V], error] {
	return s.RangeWithOptions(badger.DefaultIteratorOptions)
}

func (s *store[K, V]) RangeWithOptions(opt badger.IteratorOptions) iter.Seq2[Pair[K, V], error] {
	return func(yield func(Pair[K, V], error) bool) {
		for pair, err := range badgerRange(s.db, opt) {
			if err != nil {
				if !yield(Pair[K, V]{}, err) {
					return
				}
				continue
			}

			var v V
			err = json.Unmarshal(pair.Value, &v)
			if err != nil {
				if !yield(Pair[K, V]{}, err) {
					return
				}
				continue
			}

			if !yield(Pair[K, V]{Key: K(pair.Key), Value: v}, nil) {
				return
			}
		}
	}
}

func (s *store[K, V]) Close() error {
	return s.db.Close()
}

func NewBadgerKVBytes[K, V Bytes](dir string) (Store[K, V], error) {
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}
	return &storeBytes[K, V]{db: db}, nil
}

type storeBytes[K, V Bytes] struct {
	db *badger.DB
}

func (s *storeBytes[K, V]) Set(k K, v V) error {
	return s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return txn.Set([]byte(k), data)
	})
}

func (s *storeBytes[K, V]) Get(k K) (v V, found bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
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
		err = json.Unmarshal(data, v)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return v, found, err
}

func (s *storeBytes[K, V]) Delete(k K) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(k))
	})
}

func (s *storeBytes[K, V]) RangeWithPrefix(k K) iter.Seq2[Pair[K, V], error] {
	return s.RangeWithOptions(prefixOptions([]byte(k)))
}

func (s *storeBytes[K, V]) Range() iter.Seq2[Pair[K, V], error] {
	return s.RangeWithOptions(badger.DefaultIteratorOptions)
}

func (s *storeBytes[K, V]) RangeWithOptions(opt badger.IteratorOptions) iter.Seq2[Pair[K, V], error] {
	return func(yield func(Pair[K, V], error) bool) {
		for pair, err := range badgerRange(s.db, opt) {
			if !yield(Pair[K, V]{Key: K(pair.Key), Value: V(pair.Value)}, err) {
				return
			}
			continue
		}
	}
}

func (s *storeBytes[K, V]) Close() error {
	return s.db.Close()
}

func NewBadgerKVMarhsler[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]](dir string) (Store[K, V], error) {
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, err
	}
	return &storeInterface[K, V, KP]{db: db}, nil
}

type storeInterface[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]] struct {
	db *badger.DB
}

func (s *storeInterface[K, V, KP]) Set(k K, v V) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return txn.Set(kb, data)
	})
}

func (s *storeInterface[K, V, KP]) Get(k K) (v V, found bool, err error) {
	kb, err := k.MarshalBinary()
	if err != nil {
		return v, found, err
	}

	err = s.db.View(func(txn *badger.Txn) error {
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
		err = json.Unmarshal(data, &v)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return v, found, err
}

func (s *storeInterface[K, V, KP]) Delete(k K) error {
	kb, err := k.MarshalBinary()
	if err != nil {
		return err
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(kb)
	})
}

func (s *storeInterface[K, V, KP]) RangeWithPrefix(prefix K) iter.Seq2[Pair[K, V], error] {
	return func(yield func(Pair[K, V], error) bool) {
		p, err := prefix.MarshalBinary()
		if err != nil {
			yield(Pair[K, V]{}, err)
			return
		}

		yieldSpread2(s.RangeWithOptions(prefixOptions(p)), yield)
	}
}

func (s *storeInterface[K, V, KP]) Range() iter.Seq2[Pair[K, V], error] {
	return s.RangeWithOptions(badger.DefaultIteratorOptions)
}

func (s *storeInterface[K, V, KP]) RangeWithOptions(opt badger.IteratorOptions) iter.Seq2[Pair[K, V], error] {
	return func(yield func(Pair[K, V], error) bool) {
		for pair, err := range badgerRange(s.db, opt) {
			if err != nil {
				if !yield(Pair[K, V]{}, err) {
					return
				}
				continue
			}

			var kp KP
			err = kp.UnmarshalBinary(pair.Key)
			if err != nil {
				if !yield(Pair[K, V]{}, err) {
					return
				}
				continue
			}

			var v V
			err = json.Unmarshal(pair.Value, &v)
			if err != nil {
				if !yield(Pair[K, V]{}, err) {
					return
				}
				continue
			}

			if !yield(Pair[K, V]{}, err) {
				break
			}
			continue

		}
	}
}

func (s *storeInterface[K, V, KP]) Close() error {
	return s.db.Close()
}

func yieldSpread2[K, V any](iter iter.Seq2[K, V], yield func(K, V) bool) {
	for k, v := range iter {
		if !yield(k, v) {
			return
		}
		continue
	}
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

func badgerRange(db *badger.DB, opt badger.IteratorOptions) iter.Seq2[Pair[[]byte, []byte], error] {
	return func(yield func(Pair[[]byte, []byte], error) bool) {
		err := db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(opt)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()

				data, err := item.ValueCopy(nil)
				if err != nil {
					if !yield(Pair[[]byte, []byte]{}, err) {
						return nil
					}
					continue
				}
				if !yield(Pair[[]byte, []byte]{Key: item.KeyCopy(nil), Value: data}, nil) {
					return nil
				}
			}

			return nil
		})
		if err != nil {
			yield(Pair[[]byte, []byte]{}, err)
		}
	}
}
