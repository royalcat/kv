package kvbadger

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"
	"github.com/royalcat/kv"
)

func txGet[V any](txn *badger.Txn, k []byte, codec kv.Codec) (V, bool, error) {
	var v V

	item, err := txn.Get([]byte(k))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return v, false, nil
		}
		return v, false, err
	}

	err = item.Value(func(val []byte) error {
		return codec.Unmarshal(val, &v)
	})
	if err != nil {
		return v, true, err
	}

	return v, true, nil
}

func txSet[V any](txn *badger.Txn, k []byte, v V, codec kv.Codec) error {

	data, err := codec.Marshal(v)
	if err != nil {
		return err
	}

	return txn.Set([]byte(k), data)
}

func txRange[V any](ctx context.Context, txn *badger.Txn, opt badger.IteratorOptions, codec kv.Codec, iter kv.Iter[[]byte, V]) error {
	it := txn.NewIterator(opt)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		item := it.Item()

		var v V
		err := item.Value(func(val []byte) error {
			return codec.Unmarshal(val, &v)
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
	if _, ok := v.(*[]byte); ok {
		return errors.New("output value must be the pointer to []byte")
	}
	v = &data
	return nil
}
