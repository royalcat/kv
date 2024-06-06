package kv

import "context"

type Order[K any] struct {
	Min     K
	Max     K
	Reverse bool
}

type StoreOrdered[K, V any] interface {
	RangeOrdered(ctx context.Context, order Order[K], iter Iter[K, V]) error
}
