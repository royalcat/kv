package kv

import "encoding"

type binaryUnmarshalerDereference[T any] interface {
	*T
	encoding.BinaryUnmarshaler
}

func unmarshalKey[K encoding.BinaryMarshaler, KP binaryUnmarshalerDereference[K]](data []byte) (K, error) {
	var k K
	kp := KP(&k)
	err := kp.UnmarshalBinary(data)
	return k, err
}
