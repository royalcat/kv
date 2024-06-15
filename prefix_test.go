package kv_test

import (
	"context"
	"testing"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvmemory"
)

func TestPrefixBytes(t *testing.T) {
	m := kvmemory.NewMemoryKV[string, string]()
	pm := kv.PrefixBytes[string, string](m, "prefix-")
	ctx := context.Background()

	err := pm.Set(ctx, "key", "value")
	if err != nil {
		t.Fatal(err)
	}

	val, err := m.Get(ctx, "prefix-key")
	if err != nil {
		t.Fatal(err)
	}

	if val != "value" {
		t.Fatalf("expected value to be 'value', got %s", val)
	}

	vals := []string{}
	pm.Range(ctx, func(k, v string) error {
		vals = append(vals, v)
		return nil
	})
	if len(vals) != 1 {
		t.Fatalf("expected 1 value, got %d", len(vals))
	}
	if vals[0] != "value" {
		t.Fatalf("expected value to be 'value', got %s", vals[0])
	}

	vals = []string{}
	pm.RangeWithPrefix(ctx, "k", func(k, v string) error {
		vals = append(vals, v)
		return nil
	})
	if len(vals) != 1 {
		t.Fatalf("expected 1 value, got %d", len(vals))
	}
	if vals[0] != "value" {
		t.Fatalf("expected value to be 'value', got %s", vals[0])
	}

	err = pm.Edit(ctx, "key", func(ctx context.Context, v string) (string, error) {
		return v + "!", nil
	})
	if err != nil {
		t.Fatal(err)
	}

	val, err = m.Get(ctx, "prefix-key")
	if err != nil {
		t.Fatal(err)
	}

	if val != "value!" {
		t.Fatalf("expected value to be 'value!', got %s", val)
	}

	err = pm.Delete(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}

	_, err = pm.Get(ctx, "key")
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
