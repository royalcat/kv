package kv

import (
	"bytes"
	"context"
	"encoding"
	"strings"
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

func (s *prefixBytesStore[K, V]) cutPrefix(k K) K {
	return K(strings.TrimPrefix(string(k), string(s.prefix)))
}

// Close implements Store.
func (p *prefixBytesStore[K, V]) Close(ctx context.Context) error {
	return nil
}

// Delete implements Store.
func (p *prefixBytesStore[K, V]) Delete(ctx context.Context, k K) error {
	return p.store.Delete(ctx, p.withPrefix(k))
}

// Get implements Store.
func (p *prefixBytesStore[K, V]) Get(ctx context.Context, k K) (V, error) {
	return p.store.Get(ctx, p.withPrefix(k))
}

// Range implements Store.
func (p *prefixBytesStore[K, V]) Range(ctx context.Context, iter Iter[K, V]) error {
	return p.store.RangeWithPrefix(ctx, K(p.prefix), func(k K, v V) error {
		return iter(p.cutPrefix(k), v)
	})
}

// RangeWithPrefix implements Store.
func (p *prefixBytesStore[K, V]) RangeWithPrefix(ctx context.Context, k K, iter Iter[K, V]) error {
	return p.store.RangeWithPrefix(ctx, p.withPrefix(k), func(k K, v V) error {
		return iter(p.cutPrefix(k), v)
	})
}

// Get implements Store.
func (p *prefixBytesStore[K, V]) Edit(ctx context.Context, k K, edit Edit[V]) error {
	return p.store.Edit(ctx, p.withPrefix(k), edit)
}

// Set implements Store.
func (p *prefixBytesStore[K, V]) Set(ctx context.Context, k K, v V) error {
	return p.store.Set(ctx, p.withPrefix(k), v)
}

func PrefixBinary[K encoding.BinaryMarshaler, V any, KP binaryUnmarshalerDereference[K]](s Store[K, V], prefix K) (Store[K, V], error) {
	p, err := prefix.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &prefixBinaryStore[K, V, KP]{
		prefix: p,
		store:  s,
	}, nil
}

type prefixBinaryStore[K encoding.BinaryMarshaler, V any, KP binaryUnmarshalerDereference[K]] struct {
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

func (s *prefixBinaryStore[K, V, KP]) cutPrefix(k K) (K, error) {
	p, err := k.MarshalBinary()
	if err != nil {
		var out K
		return out, err
	}

	p = bytes.TrimPrefix(p, s.prefix)

	return unmarshalKey[K, KP](p)
}

// Close implements Store.
func (p *prefixBinaryStore[K, V, KP]) Close(ctx context.Context) error {
	return nil
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
func (p *prefixBinaryStore[K, V, KP]) Edit(ctx context.Context, k K, edit Edit[V]) error {
	pk, err := p.withPrefix(k)
	if err != nil {
		return err
	}
	return p.store.Edit(ctx, pk, edit)
}

// Get implements Store.
func (p *prefixBinaryStore[K, V, KP]) Get(ctx context.Context, k K) (V, error) {
	pk, err := p.withPrefix(k)
	if err != nil {
		var v V
		return v, err
	}
	return p.store.Get(ctx, pk)
}

// Range implements Store.
func (p *prefixBinaryStore[K, V, KP]) Range(ctx context.Context, iter Iter[K, V]) error {
	pk, err := unmarshalKey[K, KP](p.prefix)
	if err != nil {
		return err
	}
	iterCut := func(k K, v V) error {
		k, err := p.cutPrefix(k)
		if err != nil {
			return err
		}
		return iter(k, v)
	}
	return p.store.RangeWithPrefix(ctx, pk, iterCut)
}

// RangeWithPrefix implements Store.
func (p *prefixBinaryStore[K, V, KP]) RangeWithPrefix(ctx context.Context, k K, iter Iter[K, V]) error {
	pk, err := p.withPrefix(k)
	if err != nil {
		return err
	}
	iterCut := func(k K, v V) error {
		k, err := p.cutPrefix(k)
		if err != nil {
			return err
		}
		return iter(k, v)
	}

	return p.store.RangeWithPrefix(ctx, pk, iterCut)
}

// Set implements Store.
func (p *prefixBinaryStore[K, V, KP]) Set(ctx context.Context, k K, v V) error {
	pk, err := p.withPrefix(k)
	if err != nil {
		return err
	}
	return p.store.Set(ctx, pk, v)
}
