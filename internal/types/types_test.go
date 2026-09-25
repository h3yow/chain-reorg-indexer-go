package types_test

import (
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

func TestIsConfirmed(t *testing.T) {
	r := types.ConfirmationRule{Confirmations: 3}
	cases := []struct {
		block, tip uint64
		want       bool
	}{
		{0, 0, false},
		{0, 2, false},
		{0, 3, true},
		{5, 8, true},
		{5, 7, false},
		{9, 5, false}, // tip behind block
	}
	for _, c := range cases {
		if got := r.IsConfirmed(c.block, c.tip); got != c.want {
			t.Fatalf("block=%d tip=%d got=%v want=%v", c.block, c.tip, got, c.want)
		}
	}
}

func TestEventKeyUnique(t *testing.T) {
	a := types.Event{Index: 2, SourceHash: "h1"}
	b := types.Event{Index: 2, SourceHash: "h1"}
	c := types.Event{Index: 3, SourceHash: "h1"}
	if a.Key() != b.Key() || a.Key() == c.Key() {
		t.Fatal("event keys not distinguished correctly")
	}
}
