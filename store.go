package kv

import (
	"context"
	"encoding"
	"errors"
)

var ErrKeyNotFound = errors.New("key not found")

// Store is an interface that represents a key-value store.
// It provides methods for storing, retrieving, and deleting key-value pairs.
// The implementation of the store can use different marshalling formats, such as JSON or gob.
// Generic type of the value must be non-pointer type, until implementation says otherwise.
type Store[K any, V any] interface {
	// Set stores the given value for the given key.
	// The implementation automatically marshalls the value.
	// The marshalling format depends on the implementation. It can be JSON, gob, etc.
	Set(ctx context.Context, k K, v V) error

	// Get retrieves the value for the given key.
	// The implementation automatically unmarshalls the value.
	// The unmarshalling source depends on the implementation. It can be JSON, gob, etc.
	// The automatic unmarshalling requires a pointer to an object of the correct type
	// being passed as a parameter.
	// In the case of a struct, the Get method will populate the fields of the object
	// that the passed pointer points to with the values of the retrieved object's values.
	// If no value is found, it returns (false, nil).
	Get(ctx context.Context, k K) (V, error)

	// Delete deletes the stored value for the given key.
	// Deleting a non-existing key-value MUST NOT lead to an error.
	Delete(ctx context.Context, k K) error

	// Edit retrieves the value for the given key, calls the provided edit function with the value,
	Edit(ctx context.Context, k K, edit Edit[V]) error

	// Close must be called when the work with the key-value store is done.
	//
	// As the same interface is used for managing transactions, calling Close() will commit the transaction in this case.
	// Most other implementations are meant to be long-lived, so only call Close() at the very end.
	//
	// Some implementations might not need the store to be closed,
	// but as long as you work with the kv.Store interface, you never know which implementation
	// is passed to your method, so you should always call it.
	Close(ctx context.Context) error

	// Range iterates over all key-value pairs in the store and calls the provided iterator function for each pair.
	// The iterator function should return non-nil error to stop the iteration, is this case. This error will be returned by Range, its canonical to return [io.EOF]
	Range(ctx context.Context, iter Iter[K, V]) error

	// RangeWithPrefix iterates over all key-value pairs in the store that have the given prefix
	// and calls the provided iterator function for each pair.
	// The iterator function should return non-nil error to stop the iteration.
	RangeWithPrefix(ctx context.Context, prefix K, iter Iter[K, V]) error
}

// Bytes is an interface that represents a byte slice or a string.
type Bytes interface {
	~[]byte | ~string
}

// Binary is an interface that represents a binary marshaler and unmarshaler.
type Binary interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

// Iter is a function type that represents an iterator function for key-value pairs.
type Iter[K, V any] func(k K, v V) error
type Edit[V any] func(ctx context.Context, v V) (V, error)
