package kv

import (
	"context"
)

func Prefix[K Bytes, V any](s Store[K, V], prefix K) Store[K, V] {
	return &prefixStore[K, V]{
		prefix: prefix,
		store:  s,
	}
}

type prefixStore[K Bytes, V any] struct {
	prefix K
	store  Store[K, V]
}

func (s *prefixStore[K, V]) withPrefix(k K) K {
	return K(string(s.prefix) + string(k))
}

// Close implements Store.
func (p *prefixStore[K, V]) Close(ctx context.Context) error {
	return p.store.Close(ctx)
}

// Delete implements Store.
func (p *prefixStore[K, V]) Delete(ctx context.Context, k K) error {
	return p.store.Delete(ctx, p.withPrefix(k))
}

// Get implements Store.
func (p *prefixStore[K, V]) Get(ctx context.Context, k K) (v V, found bool, err error) {
	return p.store.Get(ctx, p.withPrefix(k))
}

// Range implements Store.
func (p *prefixStore[K, V]) Range(ctx context.Context, iter Iter[K, V]) error {
	return p.store.RangeWithPrefix(ctx, K(p.prefix), iter)
}

// RangeWithPrefix implements Store.
func (p *prefixStore[K, V]) RangeWithPrefix(ctx context.Context, k K, iter Iter[K, V]) error {
	return p.store.RangeWithPrefix(ctx, p.withPrefix(k), iter)
}

// Set implements Store.
func (p *prefixStore[K, V]) Set(ctx context.Context, k K, v V) error {
	return p.store.Set(ctx, k, v)
}
