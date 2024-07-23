package testsuite

import (
	"context"
	"testing"
)

func FuzzPrefixBytes(t *testing.F, newKV StoreConstructor[string, string]) {
	ctx := context.Background()

	t.Add("prefix/", "key", "value")
	t.Add("prefix-", "123", "456")
	t.Add("prefix_", "abc", "xyz")
	t.Add(string("0"), string("\xff"), string("0"))

	t.Fuzz(func(t *testing.T, prefix, key, value string) {
		store, err := newKV()
		if err != nil {
			t.Fatal(err)
		}
		testPrefixBytes(t, ctx, store, prefix, key, value)
	})
}
