package kvbitcask_test

import (
	"path"
	"testing"

	"github.com/royalcat/kv/kvbitcask"
	"github.com/royalcat/kv/testsuite"
)

func TestGolden(t *testing.T) {
	store, err := kvbitcask.New[string, string](path.Join(t.TempDir(), "bitcask"))
	if err != nil {
		t.Fatal(err)
	}
	testsuite.Golden(t, store)
}
