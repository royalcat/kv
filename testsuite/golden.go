package testsuite

import (
	"context"
	"testing"

	"github.com/royalcat/kv"
	"github.com/stretchr/testify/require"
)

type StoreConstructor func() (kv.Store[string, string], error)

func Golden(t *testing.T, newKV StoreConstructor) {
	ctx := context.Background()
	t.Run("Set Get", func(t *testing.T) {
		require := require.New(t)
		store, err := newKV()
		require.NoError(err)

		testSetGet(t, ctx, store, "key", "value")
	})
	t.Run("Prefix", func(t *testing.T) {
		require := require.New(t)
		store, err := newKV()
		require.NoError(err)

		testPrefixBytes(t, ctx, store, "prefix", "key", "value")
	})
}

func testSetGet(t *testing.T, ctx context.Context, store kv.Store[string, string], key, value string) {
	require := require.New(t)

	err := store.Set(ctx, key, value)
	require.NoError(err)

	v, err := store.Get(ctx, key)
	require.NoError(err)
	require.Equal(value, v)
}

const editSuffix = "!"

func testPrefixBytes(t *testing.T, ctx context.Context, store kv.Store[string, string], prefix, key, value string) {
	pm := kv.PrefixBytes[string, string](store, prefix)

	err := pm.Set(ctx, key, value)
	if err != nil {
		t.Fatal(err)
	}

	val, err := store.Get(ctx, prefix+key)
	if err != nil {
		t.Fatal(err)
	}
	if val != value {
		t.Fatalf("expected value to be 'value', got %s", val)
	}

	val, err = pm.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if val != value {
		t.Fatalf("expected value to be 'value', got %s", val)
	}

	vals := map[string]string{}
	err = pm.Range(ctx, func(k, v string) error {
		vals[k] = v
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected Range error: %v", err)
	}
	if len(vals) != 1 {
		t.Fatalf("expected 1 value, got %d", len(vals))
	}
	if _, ok := vals[key]; !ok {
		t.Fatalf("expected key to be 'key'")
	}
	if vals[key] != value {
		t.Fatalf("expected value to be 'value', got %s", vals[key])
	}

	if len(key) > 1 {
		vals = map[string]string{}
		err = pm.RangeWithPrefix(ctx, string([]byte{key[0]}), func(k, v string) error {
			vals[k] = v
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected RangeWithPrefix error: %v", err)
		}
		if len(vals) != 1 {
			t.Fatalf("expected 1 value, got %d", len(vals))
		}
		if _, ok := vals[key]; !ok {
			t.Fatalf("expected key to be 'key'")
		}
		if vals[key] != value {
			t.Fatalf("expected value to be 'value', got %s", vals[key])
		}
	}

	if len(key) > 1 {
		vals = map[string]string{}
		err = pm.Range(ctx, func(k, v string) error {
			vals[k] = v
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected Range error: %v", err)
		}
		if len(vals) != 1 {
			t.Fatalf("expected 1 value, got %d", len(vals))
		}
		if _, ok := vals[key]; !ok {
			t.Fatalf("expected key to be 'key'")
		}
		if vals[key] != value {
			t.Fatalf("expected value to be 'value', got %s", vals[key])
		}
	}

	err = pm.Edit(ctx, key, func(ctx context.Context, v string) (string, error) {
		return v + editSuffix, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	val, err = pm.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if val != value+editSuffix {
		t.Fatalf("expected value to be 'value!', got %s", val)
	}

	err = pm.Delete(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = pm.Get(ctx, key)
	if err != kv.ErrKeyNotFound {
		t.Fatalf("expected key not found, got %v", err)
	}

	err = pm.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
