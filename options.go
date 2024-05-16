package kv

type Options struct {
	Codec Codec
}

var DefaultOptions = Options{
	Codec: JSONcodec{},
}
