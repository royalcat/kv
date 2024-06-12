package kvolric

import (
	"context"
	"errors"
	"log"

	"github.com/buraksezer/olric"
	"github.com/royalcat/kv"
)

func NewEmbedded[V any](db *olric.Olric, bucket string) (kv.Store[string, V], error) {
	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap(bucket)
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}

	return &embedded[V]{
		c:  e,
		dm: dm,
	}, nil

}

type Options struct {
	Codec kv.Codec
}

type embedded[V any] struct {
	Options
	c  *olric.EmbeddedClient
	dm olric.DMap
}

var _ kv.Store[string, struct{}] = (*embedded[struct{}])(nil)

// Delete implements kv.Store.
func (s *embedded[V]) Delete(ctx context.Context, k string) error {
	_, err := s.dm.Delete(ctx, k)
	return err
}

// Get implements kv.Store.
func (s *embedded[V]) Get(ctx context.Context, k string) (v V, found bool, err error) {
	resp, err := s.dm.Get(ctx, k)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return v, false, nil
		}

		return v, found, err
	}

	data, err := resp.Byte()
	if err != nil {
		return v, found, err
	}

	err = s.Codec.Unmarshal(data, &v)
	return v, true, err
}

// Range implements kv.Store.
func (s *embedded[V]) Range(ctx context.Context, iter kv.Iter[string, V]) error {
	it, err := s.dm.Scan(ctx)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		k := it.Key()

		v, found, err := s.Get(ctx, k)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		if err := iter(k, v); err != nil {
			return err
		}

	}

	return nil
}

// RangeWithPrefix implements kv.Store.
func (s *embedded[V]) RangeWithPrefix(ctx context.Context, k string, iter kv.Iter[string, V]) error {
	it, err := s.dm.Scan(ctx, olric.Match("^"+k))
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		k := it.Key()

		v, found, err := s.Get(ctx, k)
		if err != nil {
			return err
		}
		if !found {
			continue
		}

		if err := iter(k, v); err != nil {
			return err
		}

	}

	return nil
}

// Set implements kv.Store.
func (s *embedded[V]) Set(ctx context.Context, k string, v V) error {
	data, err := s.Codec.Marshal(v)
	if err != nil {
		return err
	}

	return s.dm.Put(ctx, k, data)
}

// Close implements kv.Store.
func (s *embedded[V]) Close(ctx context.Context) error {
	return s.c.Close(ctx)
}
