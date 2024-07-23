package kvbbolt_test

import (
	"path"
	"testing"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvbbolt"
	"github.com/royalcat/kv/testsuite"
	"go.etcd.io/bbolt"
)

func newKV(tempDir func() string) func() (kv.Store[string, string], error) {
	return func() (kv.Store[string, string], error) {
		db, err := bbolt.Open(path.Join(tempDir(), "test.db"), 0600, nil)
		if err != nil {
			return nil, err
		}

		return kvbbolt.NewBytes[string, string](db, []byte("test")), nil
	}
}

func TestGolden(t *testing.T) {
	t.Parallel()
	testsuite.Golden(t, newKV(t.TempDir))
}
