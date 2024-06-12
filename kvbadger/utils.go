package kvbadger

import (
	"context"
	"errors"
	"reflect"
	"slices"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

func txGet[V any](txn *badger.Txn, k []byte, opts Options) (V, bool, error) {
	var v V

	item, err := txn.Get([]byte(k))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return v, false, nil
		}
		return v, false, err
	}

	err = item.Value(func(val []byte) error {
		err = opts.Codec.Unmarshal(val, &v)
		return err
	})
	if err != nil {
		return v, true, err
	}

	return v, true, nil
}

func txSet[V any](txn *badger.Txn, k []byte, v V, opts Options) error {
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

func txRange[V any](ctx context.Context, txn *badger.Txn, opt badger.IteratorOptions, opts Options, iter kv.Iter[[]byte, V]) error {
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

// Special codec for raw bytes storage
var noopCodec = noopCodecDef{}

type noopCodecDef struct{}

var _ kv.Codec = (*noopCodecDef)(nil)

// Marshal implements kv.Codec.
func (noopCodecDef) Marshal(v any) ([]byte, error) {
	if o, ok := v.([]byte); ok {
		return o, nil
	}
	return nil, errors.New("input value must be of type []byte")
}

// Unmarshal implements kv.Codec.
func (noopCodecDef) Unmarshal(data []byte, v any) error {

	val := reflect.ValueOf(v).Elem()
	valType := val.Type()

	if valType.Kind() == reflect.String {
		val.Set(reflect.ValueOf(string(data)))
		return nil
	} else if valType.Kind() == reflect.Slice && valType.Elem().Kind() == reflect.Uint8 {
		out := slices.Clone(data)
		val.Set(reflect.ValueOf(out))
		return nil
	}

	return errors.New("output value must be the pointer to []byte or string")

}
