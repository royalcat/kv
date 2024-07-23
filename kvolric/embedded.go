package kvolric

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/buraksezer/olric"
	"github.com/royalcat/kv"
)

func DefaultOptions[V any]() Options[V] {
	return Options[V]{
		Codec: kv.CodecJSON[V]{},
	}
}

func NewEmbedded[V any](db *olric.Olric, bucket string, opts Options[V]) (kv.Store[string, V], error) {
	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap(bucket)
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}

	locks, err := e.NewDMap(bucket + "_locks")
	if err != nil {
		log.Fatalf("olric.NewDMap returned an error: %v", err)
	}

	return &embedded[V]{
		c:       e,
		dm:      dm,
		locks:   locks,
		Options: opts,
	}, nil

}

type Options[V any] struct {
	Codec kv.Codec[V]
}

type embedded[V any] struct {
	Options[V]
	c     *olric.EmbeddedClient
	dm    olric.DMap
	locks olric.DMap
}

var _ kv.Store[string, struct{}] = (*embedded[struct{}])(nil)

// Delete implements kv.Store.
func (s *embedded[V]) Delete(ctx context.Context, k string) error {
	_, err := s.dm.Delete(ctx, k)
	return err
}

// Get implements kv.Store.
func (s *embedded[V]) Get(ctx context.Context, k string) (V, error) {
	var v V
	resp, err := s.dm.Get(ctx, k)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return v, kv.ErrKeyNotFound
		}
		return v, err
	}

	data, err := resp.Byte()
	if err != nil {
		return v, err
	}

	err = s.Codec.Unmarshal(data, &v)
	return v, err
}

const editTimeout = 10 * time.Second

// Get implements kv.Store.
func (s *embedded[V]) Edit(ctx context.Context, k string, edit kv.Edit[V]) error {
	lc, err := s.locks.LockWithTimeout(ctx, k, editTimeout, editTimeout)
	if err != nil {
		return err
	}
	defer lc.Unlock(ctx)

	var v V
	resp, err := s.dm.Get(ctx, k)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return kv.ErrKeyNotFound
		}
		return err
	}

	data, err := resp.Byte()
	if err != nil {
		return err
	}

	err = s.Codec.Unmarshal(data, &v)

	v, err = edit(ctx, v)
	if err != nil {
		return err
	}

	data, err = s.Codec.Marshal(v)
	if err != nil {
		return err
	}
	return s.dm.Put(ctx, k, data)
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

		v, err := s.Get(ctx, k)
		if err != nil {
			if errors.Is(err, olric.ErrKeyNotFound) {
				continue
			}

			return err
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

		v, err := s.Get(ctx, k)
		if err != nil {
			if errors.Is(err, olric.ErrKeyNotFound) {
				continue
			}

			return err
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
