package kvmemory_test

import (
	"testing"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvmemory"
	"github.com/royalcat/kv/testsuite"
)

func TestGolden(t *testing.T) {
	testsuite.GoldenStrings(t, func() (kv.Store[string, string], error) {
		return kvmemory.NewMemoryKV[string, string](), nil
	})
}
