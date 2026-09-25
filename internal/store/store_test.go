package store_test

import (
	"errors"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

func mkBlock(n uint64, hash string, parent string) *types.Block {
	return &types.Block{
		Number:     n,
		Hash:       hash,
		ParentHash: parent,
		Events: []types.Event{
			{Index: 0, Kind: "Tick", SourceHash: hash, Payload: hash},
		},
	}
}

func TestPutBlock_DedupAndGapAndConflict(t *testing.T) {
	st := store.New()

	if _, err := st.PutBlock(mkBlock(0, "h0", "")); err != nil {
		t.Fatal(err)
	}
	// Re-pull identical block: duplicate, but no new record.
	dup, err := st.PutBlock(mkBlock(0, "h0", ""))
	if err != nil || !dup {
		t.Fatalf("dup=%v err=%v, want dup=true nil", dup, err)
	}
	if st.EventCount() != 1 {
		t.Fatalf("events=%d want 1 (no duplicates on re-pull)", st.EventCount())
	}

	// Height jump: must be rejected with ErrGap.
	_, err = st.PutBlock(mkBlock(2, "h2", "h1"))
	if !errors.Is(err, store.ErrGap) {
		t.Fatalf("err=%v want ErrGap", err)
	}

	// Different hash at the same height: conflict.
	_, err = st.PutBlock(mkBlock(0, "h0-other", ""))
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("err=%v want ErrConflict", err)
	}
}

func TestRollbackAndFinalize(t *testing.T) {
	st := store.New()
	for i, h := range []string{"h0", "h1", "h2", "h3"} {
		parent := ""
		if i > 0 {
			parent = []string{"h0", "h1", "h2"}[i-1]
		}
		if _, err := st.PutBlock(mkBlock(uint64(i), h, parent)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Finalize(0); err != nil {
		t.Fatal(err)
	}
	if err := st.Finalize(1); err != nil {
		t.Fatal(err)
	}

	// Rollback above finalized head works; events of the branch disappear.
	removed, err := st.RollbackTo(1)
	if err != nil || removed != 2 {
		t.Fatalf("removed=%d err=%v want 2 nil", removed, err)
	}
	if st.EventCount() != 2 {
		t.Fatalf("events after rollback=%d want 2", st.EventCount())
	}
	if head, ok := st.Head(); !ok || head != 1 {
		t.Fatalf("head=%d,%v want 1", head, ok)
	}
	if st.HasEvent("h2#0") {
		t.Fatal("discarded branch event must not remain")
	}

	// Rolling back finalized blocks is rejected explicitly.
	_, err = st.RollbackTo(0)
	if !errors.Is(err, store.ErrFinalizedRollback) {
		t.Fatalf("err=%v want ErrFinalizedRollback", err)
	}
}

func TestReset(t *testing.T) {
	st := store.New()
	for i := 0; i < 3; i++ {
		if _, err := st.PutBlock(mkBlock(uint64(i), "h", "p")); err != nil {
			t.Fatal(err)
		}
	}
	if n := st.Reset(); n != 3 {
		t.Fatalf("reset removed=%d want 3", n)
	}
	if _, ok := st.Head(); ok {
		t.Fatal("store should be empty after reset")
	}
}
