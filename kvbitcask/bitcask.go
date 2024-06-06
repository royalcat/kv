package kvbitcask

import (
	"context"
	"errors"

	"github.com/royalcat/kv"
	"go.mills.io/bitcask/v2"
)

type BitcaskStore[K, V kv.Bytes] struct {
	DB bitcask.DB
}

func New[K, V kv.Bytes](path string, options ...bitcask.Option) (*BitcaskStore[K, V], error) {
	db, err := bitcask.Open(path, options...)
	if err != nil {
		return nil, err
	}

	return &BitcaskStore[K, V]{
		DB: db,
	}, nil
}

var _ kv.Store[string, string] = (*BitcaskStore[string, string])(nil)

// Get implements kv.Store.
func (s *BitcaskStore[K, V]) Get(ctx context.Context, k K) (v V, found bool, err error) {
	data, err := s.DB.Get(bitcask.Key(k))
	if err != nil {
		if err == bitcask.ErrKeyNotFound {
			return v, false, nil
		}

		return v, false, nil
	}

	return V(data), true, nil
}

// Set implements kv.Store.
func (s *BitcaskStore[K, V]) Set(ctx context.Context, k K, v V) error {
	return s.DB.Put(bitcask.Key(k), bitcask.Value(v))
}

func (s *BitcaskStore[K, V]) Delete(ctx context.Context, k K) error {
	return s.DB.Delete(bitcask.Key(k))
}

func (s *BitcaskStore[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	it := s.DB.Iterator()
	defer it.Close()

	for {
		item, err := it.Next()
		if err == bitcask.ErrStopIteration {
			break
		}

		if err != nil {
			return err
		}

		if item == nil {
			break
		}

		err = iter(K(item.Key()), V(item.Value()))
		if err != nil {
			break
		}
	}

	return nil
}

var iterStop = errors.New("stop")

// RangeWithPrefix implements kv.Store.
func (s *BitcaskStore[K, V]) RangeWithPrefix(ctx context.Context, k K, iter kv.Iter[K, V]) error {
	return s.DB.Scan(bitcask.Key(k), func(k bitcask.Key) error {
		v, err := s.DB.Get(k)
		if err != nil {
			return err
		}

		return iter(K(k), V(v))
	})
}

func (s *BitcaskStore[K, V]) Close(ctx context.Context) error {
	return s.DB.Close()
}
