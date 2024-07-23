package kvbadger_test

import (
	"context"
	"testing"

	"github.com/royalcat/kv/kvbadger"
	"github.com/royalcat/kv/testsuite"
	"github.com/stretchr/testify/require"
)

func TestGeneral(t *testing.T) {
	require := require.New(t)
	opts := kvbadger.DefaultOptions[string]("")
	opts.BadgerOptions.InMemory = true
	store, err := kvbadger.NewBadgerKVBytes[string, string](opts)
	require.NoError(err)
	testsuite.Golden(t, store)
}

func TestBytesStore(t *testing.T) {
	t.Run("String value", func(t *testing.T) {
		ctx := context.Background()
		require := require.New(t)
		opts := kvbadger.DefaultOptions[string]("")
		opts.BadgerOptions.InMemory = true
		str, err := kvbadger.NewBadgerKVBytes[string, string](opts)
		require.NoError(err)

		err = str.Set(ctx, "key", "value")
		require.NoError(err)

		v, err := str.Get(ctx, "key")
		require.NoError(err)
		require.Equal("value", v)
	})

	t.Run("Bytes value", func(t *testing.T) {
		ctx := context.Background()
		require := require.New(t)
		opts := kvbadger.DefaultOptions[[]byte]("")
		opts.BadgerOptions.InMemory = true
		str, err := kvbadger.NewBadgerKVBytes[string, []byte](opts)
		require.NoError(err)

		err = str.Set(ctx, "key", []byte("value"))
		require.NoError(err)

		v, err := str.Get(ctx, "key")
		require.NoError(err)
		require.Equal([]byte("value"), v)
	})

}
