package testsuite

import (
	"context"
	"testing"

	"github.com/royalcat/kv"
	"github.com/stretchr/testify/require"
)

type LocksConstructor[K any] func() (kv.Locks[K], error)

func GoldenLocks(t *testing.T, newLocks LocksConstructor[string]) {
	ctx := context.Background()
	t.Run("Lock", func(t *testing.T) {
		require := require.New(t)
		store, err := newLocks()
		require.NoError(err)

		testLock(t, ctx, store)
	})
}

func testLock(t *testing.T, ctx context.Context, store kv.Locks[string]) {
	require := require.New(t)

	err := store.Lock(ctx, "key")
	require.NoError(err)

	err = store.Unlock(ctx, "key")
	require.NoError(err)
}
