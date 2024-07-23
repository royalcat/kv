package kvbitcask_test

import (
	"path"
	"testing"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvbitcask"
	"github.com/royalcat/kv/testsuite"
)

func TestGolden(t *testing.T) {
	testsuite.Golden(t, func() (kv.Store[string, string], error) {
		return kvbitcask.New[string, string](path.Join(t.TempDir(), "bitcask"))
	})
}
