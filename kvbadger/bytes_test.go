package kvbadger_test

import (
	"testing"

	"github.com/royalcat/kv"
	"github.com/royalcat/kv/kvbadger"
	"github.com/royalcat/kv/testsuite"
)

func newMemoryBytes[V any]() (kv.Store[string, V], error) {
	opts := kvbadger.DefaultOptions[V]("")
	opts.BadgerOptions.InMemory = true
	return kvbadger.New[string, V](opts)
}

func newMemoryObjects[V any]() (kv.Store[string, V], error) {
	opts := kvbadger.DefaultOptions[V]("")
	opts.BadgerOptions.InMemory = true
	return kvbadger.New[string, V](opts)
}

func TestGolden(t *testing.T) {
	testsuite.GoldenStrings(t, newMemoryBytes)
}

func FuzzPrefixBytes(t *testing.F) {
	testsuite.FuzzPrefixBytes(t, newMemoryBytes)
}

func TestGoldenObjects(t *testing.T) {
	testsuite.GoldenObjects(t, newMemoryObjects)
}
