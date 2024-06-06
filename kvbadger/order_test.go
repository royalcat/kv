package kvbadger

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"testing"
)

func compareUint64(t *testing.T, order binary.ByteOrder, a, b uint64) {
	l := make([]byte, 8)
	order.PutUint64(l, a)
	h := make([]byte, 8)
	order.PutUint64(h, b)

	if bytes.Compare(l, h) != cmp.Compare(a, b) {
		t.Fatalf("compare failed: %d %d", a, b)

	}
}

func FuzzOrder(f *testing.F) {
	for i := range 16 {
		for j := range 16 {
			f.Add(uint64(i), uint64(j))
		}
	}
	f.Add(uint64(0), uint64(math.MaxUint64))
	f.Add(uint64(0), uint64(math.MaxUint64)-1)
	f.Add(uint64(math.MaxUint64), uint64(0))
	f.Add(uint64(math.MaxUint64)-1, uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(math.MaxUint64)-1)
	f.Add(uint64(276), uint64(34))

	f.Fuzz(func(t *testing.T, a, b uint64) {
		compareUint64(t, binary.BigEndian, a, b)
	})
}
