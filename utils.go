package kv

import "encoding"

type binaryPointer[T any] interface {
	*T
	Binary
}

func unmarshalKey[K encoding.BinaryMarshaler, KP binaryPointer[K]](data []byte) (K, error) {
	var k K
	kp := KP(&k)
	err := kp.UnmarshalBinary(data)
	return k, err
}
