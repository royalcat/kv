package kvbadger

import (
	"context"
	"encoding"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

type binaryPointer[T any] interface {
	*T
	kv.Binary
}

func unmarshalKey[K encoding.BinaryMarshaler, KP binaryPointer[K]](data []byte) (K, error) {
	var k K
	kp := KP(&k)
	err := kp.UnmarshalBinary(data)
	return k, err
}

func txGet[V any](txn *badger.Txn, k []byte, opts Options[V]) (V, error) {
	var v V

	item, err := txn.Get([]byte(k))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return v, kv.ErrKeyNotFound
		}
		return v, err
	}

	err = item.Value(func(val []byte) error {
		err = opts.Codec.Unmarshal(val, &v)
		return err
	})
	if err != nil {
		return v, err
	}

	return v, err
}

func txSet[V any](txn *badger.Txn, k []byte, v V, opts Options[V]) error {
	data, err := opts.Codec.Marshal(v)
	if err != nil {
		return err
	}

	entry := badger.NewEntry([]byte(k), data)
	if opts.DefaultTTL > 0 {
		entry = entry.WithTTL(opts.DefaultTTL)
	}

	return txn.SetEntry(entry)
}

func txRange[V any](ctx context.Context, txn *badger.Txn, opt badger.IteratorOptions, opts Options[V], iter kv.Iter[[]byte, V]) error {
	it := txn.NewIterator(opt)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		item := it.Item()

		var v V
		err := item.Value(func(val []byte) error {
			return opts.Codec.Unmarshal(val, &v)
		})
		if err != nil {
			return err
		}
		err = iter(item.KeyCopy(nil), v)
		if err != nil {
			return err
		}
	}

	return nil
}
