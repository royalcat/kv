package kv

import "context"

type Locks[K any] interface {
	Lock(ctx context.Context, key K) error
	Unlock(ctx context.Context, key K) error

	// closing lock storage and releasing all locks
	Close(ctx context.Context) error
}
