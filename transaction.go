package kv

type TransactionalStore[K, V any] interface {
	Transaction(update bool) (Store[K, V], error)
}
