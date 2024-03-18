package kv

import (
	"context"
	"strings"
	"sync"
)

func NewMemoryKV[K Bytes, V any]() {
	return

}

type memoryKV[K Bytes, V any] struct {
	m    sync.Mutex
	data map[string]V
}

var _ Store[string, string] = (*memoryKV[string, string])(nil)

// Close implements Store.
func (m *memoryKV[K, V]) Close(ctx context.Context) error {
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
func (m *memoryKV[K, V]) Get(ctx context.Context, k K) (v V, found bool, err error) {
	m.m.Lock()
	defer m.m.Unlock()

	v, found = m.data[string(k)]
	return v, found, nil
}

// Set implements Store.
func (m *memoryKV[K, V]) Set(ctx context.Context, k K, v V) error {
	m.m.Lock()
	defer m.m.Unlock()

	m.data[string(k)] = v
	return nil
}

// Range implements Store.
func (m *memoryKV[K, V]) Range(ctx context.Context, iter Iter[K, V]) error {
	m.m.Lock()
	defer m.m.Unlock()

	for k, v := range m.data {
		if !iter(K(k), v) {
			break
		}
	}
	return nil
}

// RangeWithPrefix implements Store.
func (m *memoryKV[K, V]) RangeWithPrefix(ctx context.Context, prefix K, iter Iter[K, V]) error {
	m.m.Lock()
	defer m.m.Unlock()

	for k, v := range m.data {
		if !strings.HasPrefix(k, string(prefix)) {
			continue
		}

		if !iter(K(k), v) {
			break
		}
	}
	return nil
}
