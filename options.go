package kv

type Option func(options) options

type options struct {
	codec Codec
}

var defaultOptions = options{
	codec: JSONcodec{},
}

func getOptions(opts ...Option) options {
	o := options{}
	for _, opt := range opts {
		o = opt(o)
	}
	return o
}
