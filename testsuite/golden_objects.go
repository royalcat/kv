package testsuite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type testObject struct {
	I int
}

func GoldenObjects(t *testing.T, newKV StoreConstructor[string, testObject]) {
	ctx := context.Background()
	t.Run("Set Get", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)
		store, err := newKV()
		require.NoError(err)

		testSetGet(t, ctx, store, "key", testObject{I: 42})
	})
}
