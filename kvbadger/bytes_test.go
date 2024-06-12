package kvbadger_test

import (
	"context"
	"testing"

	"github.com/royalcat/kv/kvbadger"
	"github.com/stretchr/testify/require"
)

func TestBytesStore(t *testing.T) {
	t.Run("String value", func(t *testing.T) {
		ctx := context.Background()
		require := require.New(t)
		opts := kvbadger.DefaultOptions("")
		opts.BadgerOptions.InMemory = true
		str, err := kvbadger.NewBadgerKVBytes[string, string](opts)
		require.NoError(err)

		err = str.Set(ctx, "key", "value")
		require.NoError(err)

		v, found, err := str.Get(ctx, "key")
		require.NoError(err)
		require.True(found)
		require.Equal("value", v)
	})

	t.Run("Bytes value", func(t *testing.T) {
		ctx := context.Background()
		require := require.New(t)
		opts := kvbadger.DefaultOptions("")
		opts.BadgerOptions.InMemory = true
		str, err := kvbadger.NewBadgerKVBytes[string, []byte](opts)
		require.NoError(err)

		err = str.Set(ctx, "key", []byte("value"))
		require.NoError(err)

		v, found, err := str.Get(ctx, "key")
		require.NoError(err)
		require.True(found)
		require.Equal([]byte("value"), v)
	})

}
