package kvmemory_test

import (
	"testing"

	"github.com/royalcat/kv/kvmemory"
	"github.com/royalcat/kv/testsuite"
)

func TestGolden(t *testing.T) {
	testsuite.Golden(t, kvmemory.NewMemoryKV[string, string]())
}
