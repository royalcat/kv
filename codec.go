package kv

import (
	"bytes"
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

// GobCodec encodes/decodes Go values to/from gob.
// You can use encoding.Gob instead of creating an instance of this struct.
type GobCodec[V any] struct{}

// Marshal encodes a Go value to gob.
func (c GobCodec[V]) Marshal(v V) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Unmarshal decodes a gob value into a Go value.
func (c GobCodec[V]) Unmarshal(data []byte, v *V) error {
	reader := bytes.NewReader(data)
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(v)
}

// JSONCodec encodes/decodes Go values to/from JSON.
// You can use encoding.JSON instead of creating an instance of this struct.
type JSONCodec[V any] struct{}

// Marshal encodes a Go value to JSON.
func (c JSONCodec[V]) Marshal(v V) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal decodes a JSON value into a Go value.
func (c JSONCodec[V]) Unmarshal(data []byte, v *V) error {
	return json.Unmarshal(data, v)
}

type BytesCodec[V Bytes] struct{}

var _ Codec[[]byte] = (*BytesCodec[[]byte])(nil)

// Marshal implements kv.Codec.
func (BytesCodec[V]) Marshal(v V) ([]byte, error) {
	return slices.Clone([]byte(v)), nil
}

// Unmarshal implements kv.Codec.
func (BytesCodec[V]) Unmarshal(data []byte, v *V) error {
	*v = V(slices.Clone(data))
	return nil
}
