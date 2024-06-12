package kv

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

type Codec interface {
	// Marshal encodes a Go value to a slice of bytes.
	Marshal(v any) ([]byte, error)
	// Unmarshal decodes a slice of bytes into a Go value.
	Unmarshal(data []byte, v any) error
}

// Convenience variables
var (
	// JSON is a JSONcodec that encodes/decodes Go values to/from JSON.
	JSON = JSONCodec{}
	// Gob is a GobCodec that encodes/decodes Go values to/from gob.
	Gob = GobCodec{}
)

// GobCodec encodes/decodes Go values to/from gob.
// You can use encoding.Gob instead of creating an instance of this struct.
type GobCodec struct{}

// Marshal encodes a Go value to gob.
func (c GobCodec) Marshal(v any) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Unmarshal decodes a gob value into a Go value.
func (c GobCodec) Unmarshal(data []byte, v any) error {
	reader := bytes.NewReader(data)
	decoder := gob.NewDecoder(reader)
	return decoder.Decode(v)
}

// JSONCodec encodes/decodes Go values to/from JSON.
// You can use encoding.JSON instead of creating an instance of this struct.
type JSONCodec struct{}

// Marshal encodes a Go value to JSON.
func (c JSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal decodes a JSON value into a Go value.
func (c JSONCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
