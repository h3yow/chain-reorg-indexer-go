package indexer_test

import (
	"context"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// With zero confirmations every block is presented as final immediately.
// A later same-tip reorg must still be detected and fully corrected, and the
// result must equal indexing the new chain from genesis.
func TestReorg_ZeroConfirmationsCorrectsDeliveredBlocks(t *testing.T) {
	oldChain := fakenode.BuildLinear("A", 0, 6) // 0..5
	fork := fakenode.BuildFork("A", 4, 2)       // 4..5, same tip
	newChain := append(append([]*types.Block{}, oldChain[:4]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 0, 2, nil) // confirmations = 0

	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Everything, including the heights that will be reorged, was already
	// delivered as a final result.
	if got := len(st.FinalBlocks()); got != 6 {
		t.Fatalf("finalized=%d want 6 (zero confirmations finalize immediately)", got)
	}
	for _, h := range []uint64{4, 5} {
		if b := st.GetBlock(h); b == nil || !b.Finalized {
			t.Fatalf("height %d must be finalized before the reorg", h)
		}
	}

	// Same-tip hash swap at heights that were already finalized/delivered.
	node.ApplyReorg(4, fork...)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run after same-tip reorg with zero confirmations: %v", err)
	}

	ref := referenceIndex(t, newChain, 0)
	got, want := st.AllBlocks(), ref.AllBlocks()
	if len(got) != len(want) {
		t.Fatalf("blocks=%d want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].Hash != want[i].Hash || got[i].Finalized != want[i].Finalized {
			t.Fatalf("height %d mismatch: got (%s,fin=%v) want (%s,fin=%v)",
				got[i].Number, got[i].Hash, got[i].Finalized, want[i].Hash, want[i].Finalized)
		}
	}
	// No residue from the replaced branch.
	for _, b := range oldChain[4:] {
		if st.HasEvent(b.Events[0].Key()) {
			t.Fatalf("replaced branch event remains: %s", b.Events[0].Key())
		}
	}
	if st.EventCount() != ref.EventCount() {
		t.Fatalf("events=%d reference=%d", st.EventCount(), ref.EventCount())
	}
	t.Logf("scenario=zero-confirmations-same-tip-reorg corrected blocks=%d events=%d",
		len(got), st.EventCount())
}

// A genesis-level swap with zero confirmations (no common ancestor at all)
// must reset and rebuild, still matching a from-scratch index.
func TestReorg_ZeroConfirmationsGenesisSwap(t *testing.T) {
	oldChain := fakenode.BuildLinear("A", 0, 4)
	other := fakenode.BuildLinear("B", 0, 4)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 0, 1, nil)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(st.FinalBlocks()); got != 4 {
		t.Fatalf("finalized=%d want 4", got)
	}

	node.ApplyReorg(0, other...)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("genesis swap with zero confirmations: %v", err)
	}
	got := st.AllBlocks()
	if len(got) != 4 {
		t.Fatalf("blocks=%d want 4", len(got))
	}
	for i, b := range got {
		if b.Hash != other[i].Hash {
			t.Fatalf("height %d hash %s want %s", i, b.Hash, other[i].Hash)
		}
	}
	for _, b := range oldChain {
		if st.HasEvent(b.Events[0].Key()) {
			t.Fatalf("old chain event residue: %s", b.Events[0].Key())
		}
	}
	t.Logf("scenario=zero-confirmations-genesis-swap rebuilt blocks=%d", len(got))
}
