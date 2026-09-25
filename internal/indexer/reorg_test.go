package indexer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/indexer"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// collectEventPayloads returns event payloads of finalized+unfinalized blocks.
func collectEventPayloads(st *store.Store) []string {
	var out []string
	for _, b := range st.AllBlocks() {
		for _, e := range b.Events {
			out = append(out, e.Payload)
		}
	}
	return out
}

func referenceIndex(t *testing.T, chain []*types.Block, confs uint64) *store.Store {
	t.Helper()
	refNode := fakenode.New()
	refNode.SetChain(chain...)
	st := store.New()
	ix := indexer.New(indexer.Config{
		Client:      refNode,
		Store:       st,
		Rule:        types.ConfirmationRule{Confirmations: confs},
		Concurrency: 1,
	})
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("reference index: %v", err)
	}
	return st
}

func TestReorg_RollbackToCommonAncestorAndEquivalence(t *testing.T) {
	// Old chain A: heights 0..9; fork from height 6 (common ancestor = 5).
	oldChain := fakenode.BuildLinear("A", 0, 10)
	fork := fakenode.BuildFork("A", 6, 6) // heights 6..11 on the new branch
	newChain := append(append([]*types.Block{}, oldChain[:6]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)

	ix, st := newIndexer(t, node, 4, 3, nil)

	// Index the old chain while it is the live chain.
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldHashes := hashes(st.AllBlocks())
	if len(oldHashes) != 10 {
		t.Fatalf("old chain blocks=%d want 10", len(oldHashes))
	}

	// Node reorganizes: heights 6..9 replaced, tip advances to 11.
	node.ApplyReorg(6, fork...)

	// Incremental indexing must detect the divergence and converge onto the
	// new chain automatically.
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run after reorg: %v", err)
	}

	got := st.AllBlocks()
	if len(got) != 12 {
		t.Fatalf("blocks after reorg=%d want 12", len(got))
	}
	// Equivalence: result must equal indexing the new chain from genesis.
	ref := referenceIndex(t, newChain, 4)
	refBlocks := ref.AllBlocks()
	if len(got) != len(refBlocks) {
		t.Fatalf("got %d blocks, reference %d", len(got), len(refBlocks))
	}
	for i := range got {
		if got[i].Hash != refBlocks[i].Hash {
			t.Fatalf("height %d: got hash %s want reference %s",
				got[i].Number, got[i].Hash, refBlocks[i].Hash)
		}
		if got[i].Finalized != refBlocks[i].Finalized {
			t.Fatalf("height %d finalized mismatch", got[i].Number)
		}
	}

	// No residue: old-branch hashes/events must be gone.
	oldBranch := oldChain[6:]
	for _, b := range oldBranch {
		if rec := st.GetBlock(b.Number); rec != nil && rec.Hash == b.Hash {
			t.Fatalf("discarded branch residue at height %d hash %s", b.Number, b.Hash)
		}
		for _, e := range b.Events {
			if st.HasEvent(e.Key()) {
				t.Fatalf("discarded branch event remains: %s", e.Key())
			}
		}
	}
	if st.EventCount() != ref.EventCount() {
		t.Fatalf("events=%d reference=%d", st.EventCount(), ref.EventCount())
	}

	// Common ancestor blocks stay untouched.
	if got := st.GetBlock(5).Hash; got != oldChain[5].Hash {
		t.Fatalf("ancestor height 5 changed: %s", got)
	}
}

func TestReorg_TipRollback(t *testing.T) {
	// Node tip goes backwards: a longer old branch is abandoned wholesale.
	oldChain := fakenode.BuildLinear("A", 0, 8) // tip 7
	fork := fakenode.BuildFork("A", 4, 2)       // heights 4..5, tip 5
	newChain := append(append([]*types.Block{}, oldChain[:4]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 4, 2, nil)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	node.ApplyReorg(4, fork...) // tip drops from 7 to 5
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run after tip rollback: %v", err)
	}

	ref := referenceIndex(t, newChain, 4)
	got := st.AllBlocks()
	if len(got) != len(ref.AllBlocks()) {
		t.Fatalf("blocks=%d want %d", len(got), len(ref.AllBlocks()))
	}
	for i := range got {
		if got[i].Hash != ref.AllBlocks()[i].Hash {
			t.Fatalf("height %d mismatch", got[i].Number)
		}
	}
	for _, b := range oldChain[4:] {
		if st.HasEvent(b.Events[0].Key()) {
			t.Fatalf("abandoned event remains: %s", b.Events[0].Key())
		}
	}
}

func TestReorg_DeepReorgPastFinalityRejected(t *testing.T) {
	oldChain := fakenode.BuildLinear("A", 0, 8)
	node := fakenode.New()
	node.SetChain(oldChain...)

	// Confirmations = 3: after indexing tip 7, heights 0..4 are finalized.
	ix, st := newIndexer(t, node, 3, 2, nil)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	fh, _ := st.FinalizedHead()
	if fh != 4 {
		t.Fatalf("finalized head=%d want 4", fh)
	}

	// Reorg diverges at height 2 — deeper than the finalized head.
	fork := fakenode.BuildFork("A", 2, 7)
	node.ApplyReorg(2, fork...)
	err := ix.Run(context.Background())
	if !errors.Is(err, indexer.ErrFinalityReorg) {
		t.Fatalf("err=%v want ErrFinalityReorg", err)
	}
	t.Logf("deep reorg correctly rejected: %v", err)

	// Finalized data is untouched after the explicit failure.
	if b := st.GetBlock(2); b == nil || b.Hash != oldChain[2].Hash {
		t.Fatal("finalized block must remain intact after rejected reorg")
	}
}

func TestReorg_UnconfirmedShallowReorgSucceeds(t *testing.T) {
	// With confirmations=3 a reorg above finalized head 4 must still recover.
	oldChain := fakenode.BuildLinear("A", 0, 8) // tip 7 -> finalized 0..4
	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 3, 2, nil)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	fork := fakenode.BuildFork("A", 5, 3) // diverges at 5 (unfinalized), tip 7
	newChain := append(append([]*types.Block{}, oldChain[:5]...), fork...)
	node.ApplyReorg(5, fork...)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("shallow reorg should recover: %v", err)
	}

	ref := referenceIndex(t, newChain, 3)
	got := st.AllBlocks()
	rb := ref.AllBlocks()
	if len(got) != len(rb) {
		t.Fatalf("blocks=%d want %d", len(got), len(rb))
	}
	for i := range got {
		if got[i].Hash != rb[i].Hash || got[i].Finalized != rb[i].Finalized {
			t.Fatalf("height %d mismatch: got (%s,fin=%v) want (%s,fin=%v)",
				got[i].Number, got[i].Hash, got[i].Finalized, rb[i].Hash, rb[i].Finalized)
		}
	}
	if len(st.FinalBlocks()) != len(ref.FinalBlocks()) {
		t.Fatalf("finalized count mismatch")
	}
	_ = fmt.Sprintf // keep fmt import stable across edits
}

func TestReorg_MissingBlockInMiddleOfBatch(t *testing.T) {
	// First run indexes a short chain 0..5. Then the node reorganizes at
	// height 3 to a fork of equal tip 5. During the second run, tip has not
	// advanced (6..6 is empty) so verifyTail detects the swap directly.
	oldChain := fakenode.BuildLinear("A", 0, 6) // 0..5
	fork := fakenode.BuildFork("A", 3, 3)       // 3..5
	newChain := append(append([]*types.Block{}, oldChain[:3]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 3, 4, nil) // finalized 0..2, tail 3..5 mutable
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	node.ApplyReorg(3, fork...)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("reorg at equal tip: %v", err)
	}
	ref := referenceIndex(t, newChain, 3)
	got := st.AllBlocks()
	rb := ref.AllBlocks()
	if len(got) != len(rb) {
		t.Fatalf("blocks=%d want %d", len(got), len(rb))
	}
	for i := range got {
		if got[i].Hash != rb[i].Hash {
			t.Fatalf("height %d hash mismatch after mid-batch reorg", i)
		}
	}
}

func TestReorg_NoCommonAncestorResetsAndReindexes(t *testing.T) {
	// Two entirely different genesis identities => no common ancestor.
	oldChain := fakenode.BuildLinear("A", 0, 4)
	other := fakenode.BuildLinear("B", 0, 4)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 4, 2, nil) // nothing finalized at tip 3
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Replace the whole chain, including genesis, at the same tip height.
	node.ApplyReorg(0, other...)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("genesis-level reorg: %v", err)
	}

	got := st.AllBlocks()
	if len(got) != 4 {
		t.Fatalf("blocks=%d want 4", len(got))
	}
	for i, b := range got {
		if b.Hash != other[i].Hash {
			t.Fatalf("height %d hash %s want new-chain %s", i, b.Hash, other[i].Hash)
		}
	}
	for _, b := range oldChain {
		for _, e := range b.Events {
			if st.HasEvent(e.Key()) {
				t.Fatalf("old-chain event residue: %s", e.Key())
			}
		}
	}
}
