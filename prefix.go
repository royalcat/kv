package kv

import (
	"context"
	"encoding"
)

func PrefixBytes[K Bytes, V any](s Store[K, V], prefix K) Store[K, V] {
	return &prefixBytesStore[K, V]{
		prefix: prefix,
		store:  s,
	}
}

type prefixBytesStore[K Bytes, V any] struct {
	prefix K
	store  Store[K, V]
}

func (s *prefixBytesStore[K, V]) withPrefix(k K) K {
	return K(string(s.prefix) + string(k))
}

// Close implements Store.
func (p *prefixBytesStore[K, V]) Close(ctx context.Context) error {
	return p.store.Close(ctx)
}

// Delete implements Store.
func (p *prefixBytesStore[K, V]) Delete(ctx context.Context, k K) error {
	return p.store.Delete(ctx, p.withPrefix(k))
}

// Get implements Store.
func (p *prefixBytesStore[K, V]) Get(ctx context.Context, k K) (v V, found bool, err error) {
	return p.store.Get(ctx, p.withPrefix(k))
}

// Range implements Store.
func (p *prefixBytesStore[K, V]) Range(ctx context.Context, iter Iter[K, V]) error {
	return p.store.RangeWithPrefix(ctx, K(p.prefix), iter)
}

// RangeWithPrefix implements Store.
func (p *prefixBytesStore[K, V]) RangeWithPrefix(ctx context.Context, k K, iter Iter[K, V]) error {
	return p.store.RangeWithPrefix(ctx, p.withPrefix(k), iter)
}

// Set implements Store.
func (p *prefixBytesStore[K, V]) Set(ctx context.Context, k K, v V) error {
	return p.store.Set(ctx, k, v)
}

func PrefixBinary[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]](s Store[K, V], prefix K) (Store[K, V], error) {
	p, err := prefix.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &prefixBinaryStore[K, V, KP]{
		prefix: p,
		store:  s,
	}, nil
}

type prefixBinaryStore[K encoding.BinaryMarshaler, V any, KP binaryPointer[K]] struct {
	prefix []byte
	store  Store[K, V]
}

func (s *prefixBinaryStore[K, V, KP]) withPrefix(k K) (K, error) {
	p, err := k.MarshalBinary()
	if err != nil {
		var out K
		return out, err
	}

	return unmarshalKey[K, KP](p)
}

// Close implements Store.
func (p *prefixBinaryStore[K, V, KP]) Close(ctx context.Context) error {
	return p.store.Close(ctx)
}

// Delete implements Store.
func (p *prefixBinaryStore[K, V, KP]) Delete(ctx context.Context, k K) error {
	pk, err := p.withPrefix(k)
	if err != nil {
		return err
	}
	return p.store.Delete(ctx, pk)
}

// Get implements Store.
func (p *prefixBinaryStore[K, V, KP]) Get(ctx context.Context, k K) (v V, found bool, err error) {
	pk, err := p.withPrefix(k)
	if err != nil {
		return v, found, err
	}
	return p.store.Get(ctx, pk)
}

// Range implements Store.
func (p *prefixBinaryStore[K, V, KP]) Range(ctx context.Context, iter Iter[K, V]) error {
	pk, err := unmarshalKey[K, KP](p.prefix)
	if err != nil {
		return err
	}
	return p.store.RangeWithPrefix(ctx, pk, iter)
}

// RangeWithPrefix implements Store.
func (p *prefixBinaryStore[K, V, KP]) RangeWithPrefix(ctx context.Context, k K, iter Iter[K, V]) error {
	pk, err := p.withPrefix(k)
	if err != nil {
		return err
	}
	return p.store.RangeWithPrefix(ctx, pk, iter)
}

// Set implements Store.
func (p *prefixBinaryStore[K, V, KP]) Set(ctx context.Context, k K, v V) error {
	return p.store.Set(ctx, k, v)
}
