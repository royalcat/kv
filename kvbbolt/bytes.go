package kvbbolt

import (
	"bytes"
	"context"

	"github.com/royalcat/kv"
	"go.etcd.io/bbolt"
)

func NewBytes[K, V kv.Bytes](db *bbolt.DB, bucket []byte) *bytesStore[K, V] {
	return &bytesStore[K, V]{
		db:     db,
		bucket: bucket,
	}
}

type bytesStore[K, V kv.Bytes] struct {
	db     *bbolt.DB
	bucket []byte
}

var _ kv.Store[string, string] = (*bytesStore[string, string])(nil)

// Close implements kv.Store.
func (s *bytesStore[K, V]) Close(ctx context.Context) error {
	return s.db.Close()
}

// Delete implements kv.Store.
func (s *bytesStore[K, V]) Delete(ctx context.Context, k K) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)

		if b == nil {
			var err error
			b, err = tx.CreateBucket(s.bucket)
			if err != nil {
				return err
			}
		}

		return b.Delete([]byte(k))
	})
}

// Edit implements kv.Store.
func (s *bytesStore[K, V]) Edit(ctx context.Context, k K, edit kv.Edit[V]) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return kv.ErrKeyNotFound
		}

		val := b.Get([]byte(k))
		if val == nil {
			return kv.ErrKeyNotFound
		}

		newVal, err := edit(ctx, V(val))
		if err != nil {
			return err
		}

		return b.Put([]byte(k), []byte(newVal))
	})
}

// Set implements kv.Store.
func (s *bytesStore[K, V]) Set(ctx context.Context, k K, v V) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			var err error
			b, err = tx.CreateBucket(s.bucket)
			if err != nil {
				return err
			}
		}

		return b.Put([]byte(k), []byte(v))
	})
}

// Get implements kv.Store.
func (s *bytesStore[K, V]) Get(ctx context.Context, k K) (V, error) {
	var v V
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return kv.ErrKeyNotFound
		}

		val := b.Get([]byte(k))
		if val == nil {
			return kv.ErrKeyNotFound
		}

		v = V(val)

		return nil
	})
	return v, err
}

// Range implements kv.Store.
func (s *bytesStore[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			return iter(K(k), V(v))
		})
	})
}

// RangeWithPrefix implements kv.Store.
func (s *bytesStore[K, V]) RangeWithPrefix(ctx context.Context, prefix K, iter kv.Iter[K, V]) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return nil
		}

		cur := b.Cursor()
		k, v := cur.Seek([]byte(prefix))
		for ; k != nil && bytes.HasPrefix(k, []byte(prefix)); k, v = cur.Next() {
			if err := iter(K(k), V(v)); err != nil {
				return err
			}
		}

		return nil
	})
}
