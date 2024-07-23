package kvmemory_test

import (
	"testing"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvmemory"
	"github.com/royalcat/kv/testsuite"
)

func TestLocks(t *testing.T) {
	testsuite.GoldenLocks(t, func() (kv.Locks[string], error) {
		return kvmemory.NewLocks[string](), nil
	})
}
