package kvbadger

import (
	"encoding"

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
