package kv_test

import (
	"context"
	"testing"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvmemory"
)

func FuzzPrefixBytes(t *testing.F) {
	t.Add("prefix/", "key", "value")
	t.Add("prefix-", "123", "456")
	t.Add("prefix_", "abc", "xyz")
	t.Add(string("0"), string("\xff"), string("0"))

	t.Fuzz(testPrefixBytes)
}

const editSuffix = "!"

func testPrefixBytes(t *testing.T, prefix, key, value string) {
	m := kvmemory.NewMemoryKV[string, string]()
	pm := kv.PrefixBytes[string, string](m, prefix)
	ctx := context.Background()

	err := pm.Set(ctx, key, value)
	if err != nil {
		t.Fatal(err)
	}

	val, err := m.Get(ctx, prefix+key)
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

	err = m.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
}
