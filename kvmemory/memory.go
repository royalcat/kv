package kvmemory

import (
	"context"
	"strings"
	"sync"

	"github.com/royalcat/kv"
)

func NewMemoryKV[K kv.Bytes, V any]() kv.Store[K, V] {
	return &memoryKV[K, V]{
		data: map[string]V{},
	}
}

type memoryKV[K kv.Bytes, V any] struct {
	m    sync.Mutex
	data map[string]V
}

var _ kv.Store[string, string] = (*memoryKV[string, string])(nil)

// Close implements Store.
func (m *memoryKV[K, V]) Close(ctx context.Context) error {
	return nil
}

// Delete implements Store.
func (m *memoryKV[K, V]) Edit(ctx context.Context, k K, edit kv.Edit[V]) error {
	m.m.Lock()
	defer m.m.Unlock()

	v, found := m.data[string(k)]
	if !found {
		return kv.ErrKeyNotFound
	}
	var err error
	v, err = edit(ctx, v)
	if err != nil {
		return err
	}
	m.data[string(k)] = v
	return nil
}

// Delete implements Store.
func (m *memoryKV[K, V]) Delete(ctx context.Context, k K) error {
	m.m.Lock()
	defer m.m.Unlock()

	delete(m.data, string(k))
	return nil
}

// Get implements Store.
func (m *memoryKV[K, V]) Get(ctx context.Context, k K) (V, error) {
	m.m.Lock()
	defer m.m.Unlock()

	v, found := m.data[string(k)]
	if !found {
		return v, kv.ErrKeyNotFound
	}

	return v, nil
}

// Set implements Store.
func (m *memoryKV[K, V]) Set(ctx context.Context, k K, v V) error {
	m.m.Lock()
	defer m.m.Unlock()

	m.data[string(k)] = v
	return nil
}

// Range implements Store.
func (m *memoryKV[K, V]) Range(ctx context.Context, iter kv.Iter[K, V]) error {
	m.m.Lock()
	defer m.m.Unlock()

	for k, v := range m.data {
		if err := iter(K(k), v); err != nil {
			return err
		}
	}
	return nil
}

// RangeWithPrefix implements Store.
func (m *memoryKV[K, V]) RangeWithPrefix(ctx context.Context, prefix K, iter kv.Iter[K, V]) error {
	m.m.Lock()
	defer m.m.Unlock()

	for k, v := range m.data {
		if !strings.HasPrefix(k, string(prefix)) {
			continue
		}

		if err := iter(K(k), v); err != nil {
			return err
		}
	}
	return nil
}
