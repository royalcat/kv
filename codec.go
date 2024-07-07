package kv

import (
	"bytes"
	"encoding"
	"encoding/gob"
	"encoding/json"
	"slices"
)

type Codec[V any] interface {
	// Marshal encodes a Go value to a slice of bytes.
	Marshal(v V) ([]byte, error)
	// Unmarshal decodes a slice of bytes into a Go value.
	Unmarshal(data []byte, v *V) error
}

// CodecGob encodes/decodes Go values to/from gob.
// You can use encoding.Gob instead of creating an instance of this struct.
type CodecGob[V any] struct{}

// Marshal encodes a Go value to gob.
func (c CodecGob[V]) Marshal(v V) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Unmarshal decodes a gob value into a Go value.
func (c CodecGob[V]) Unmarshal(data []byte, v *V) error {
	reader := bytes.NewReader(data)
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(v)
}

// CodecJSON encodes/decodes Go values to/from JSON.
// You can use encoding.JSON instead of creating an instance of this struct.
type CodecJSON[V any] struct{}

// Marshal encodes a Go value to JSON.
func (c CodecJSON[V]) Marshal(v V) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal decodes a JSON value into a Go value.
func (c CodecJSON[V]) Unmarshal(data []byte, v *V) error {
	return json.Unmarshal(data, v)
}

type CodecBytes[V Bytes] struct{}

var _ Codec[[]byte] = (*CodecBytes[[]byte])(nil)

// Marshal implements kv.Codec.
func (CodecBytes[V]) Marshal(v V) ([]byte, error) {
	return slices.Clone([]byte(v)), nil
}

// Unmarshal implements kv.Codec.
func (CodecBytes[V]) Unmarshal(data []byte, v *V) error {
	*v = V(slices.Clone(data))
	return nil
}

type binaryPointer[T any] interface {
	*T
	Binary
}

type binaryExample struct{}

var _ Binary = (*binaryExample)(nil)

// MarshalBinary implements Binary.
func (b binaryExample) MarshalBinary() (data []byte, err error) {
	return nil, nil
}

// UnmarshalBinary implements Binary.
func (b *binaryExample) UnmarshalBinary(data []byte) error {
	return nil
}

// CodecBinary encodes/decodes Go values to/from binary.
type CodecBinary[V encoding.BinaryMarshaler, VP binaryPointer[V]] struct{}

var _ Codec[binaryExample] = (*CodecBinary[binaryExample, *binaryExample])(nil)

// Marshal encodes a Go value to JSON.
func (c CodecBinary[V, VP]) Marshal(v V) ([]byte, error) {
	return v.MarshalBinary()
}

// Unmarshal decodes a JSON value into a Go value.
func (c CodecBinary[V, VP]) Unmarshal(data []byte, v VP) error {
	return v.UnmarshalBinary(data)
}
