package kvbadger_test

import (
	"testing"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvbadger"
	"github.com/royalcat/kv/testsuite"
)

func newMemory() (kv.Store[string, string], error) {
	opts := kvbadger.DefaultOptions[string]("")
	opts.BadgerOptions.InMemory = true
	return kvbadger.NewBadgerKVBytes[string, string](opts)
}

func TestGolden(t *testing.T) {
	testsuite.Golden(t, newMemory)
}

func FuzzPrefixBytes(t *testing.F) {
	testsuite.FuzzPrefixBytes(t, newMemory)
}
