package kvolric

import (
	"context"
	"sync"
	"time"

	"github.com/buraksezer/olric"
	"github.com/royalcat/kv"
)

type Locks struct {
	defaultTimeout time.Duration

	dm olric.DMap

	mlock sync.Mutex
	locks map[string]olric.LockContext
}

func NewLocks(dm olric.DMap, defaultTimeout time.Duration) *Locks {
	return &Locks{
		defaultTimeout: defaultTimeout,
		dm:             dm,
		locks:          map[string]olric.LockContext{},
	}
}

var _ kv.Locks[string] = (*Locks)(nil)

// Lock implements kv.Locks.
func (l *Locks) Lock(ctx context.Context, key string) error {
	timeout := l.defaultTimeout

	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	lc, err := l.dm.Lock(ctx, key, timeout)

	l.mlock.Lock()
	l.locks[key] = lc
	l.mlock.Unlock()

	return err
}

// Unlock implements kv.Locks.
func (l *Locks) Unlock(ctx context.Context, key string) error {
	l.mlock.Lock()
	lc, ok := l.locks[key]
	if !ok {
		l.mlock.Unlock()
		return nil
	}
	delete(l.locks, key)
	l.mlock.Unlock()

	return lc.Unlock(ctx)
}

func (l *Locks) Close(ctx context.Context) error {
	l.mlock.Lock()
	defer l.mlock.Unlock()

	for key, lc := range l.locks {
		if err := lc.Unlock(ctx); err != nil {
			return err
		}
		delete(l.locks, key)
	}

	return nil
}
