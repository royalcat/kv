package kvmemory

import (
	"context"
	"fmt"
	"sync"

	"github.com/royalcat/kv"
)

type locks[K kv.Bytes] struct {
	mu    sync.RWMutex
	locks map[string]*sync.Mutex
}

func NewLocks[K kv.Bytes]() kv.Locks[K] {
	return &locks[K]{
		locks: map[string]*sync.Mutex{},
	}
}

var _ kv.Locks[string] = (*locks[string])(nil)

// Lock implements kv.Locks.
func (l *locks[K]) Lock(ctx context.Context, key K) error {
	k := string(key)

	l.mu.RLock()
	mu, ok := l.locks[k]
	l.mu.RUnlock()
	if !ok {
		l.mu.Lock()
		mu = &sync.Mutex{}
		l.locks[k] = mu
		l.mu.Unlock()
	}
	mu.Lock()
	return nil
}

// Unlock implements kv.Locks.
func (l *locks[K]) Unlock(ctx context.Context, key K) error {
	k := string(key)

	l.mu.RLock()
	mu, ok := l.locks[k]
	l.mu.RUnlock()

	if !ok {
		return fmt.Errorf("lock not found for key: %v", key)
	}
	mu.Unlock()
	return nil
}

func (l *locks[K]) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, mu := range l.locks {
		mu.Unlock()
	}
	return nil

}
