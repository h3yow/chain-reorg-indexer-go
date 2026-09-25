package indexer

import (
	"errors"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
)

// A reorg affecting only unconfirmed blocks must be transparent: after the
// node swaps its tail, the window rebuilds on the new fork and confirmed
// output never contains old-fork data.
func TestReorgWithinUnconfirmedWindow(t *testing.T) {
	chain := fakenode.BuildChain(0, 5, "", "a") // 0..4
	h := newHarness(t, chain, Config{Confirmations: 2, MaxConcurrency: 3, MaxReorgDepth: 10})
	if err := h.runUntilIdle(); err != nil {
		t.Fatal(err)
	}
	h.mustFinalHeights([]uint64{0, 1, 2})

	// Replace 3..4 with b-fork and extend the fork to height 5.
	fork := fakenode.BuildChain(3, 3, h.node.CanonicalHash(2), "b") // 3,4,5
	h.node.SetReorg(2, fork)
	if err := h.runUntilIdle(); err != nil {
		t.Fatalf("reorg within window: %v", err)
	}
	// tip=5, conf=2 -> final up to 3; height 3 must be the b block.
	h.mustFinalHeights([]uint64{0, 1, 2, 3})
	b3, err := h.st.BlockAt(3)
	if err != nil || b3.Hash != "0xb-3" {
		t.Fatalf("height 3 = %+v, want 0xb-3", b3)
	}
}

// A reorg that replaces already-confirmed blocks: indexer must rollback to
// the common ancestor and re-index. Final state must equal indexing the new
// chain from scratch, with zero leftover old-fork rows.
func TestReorgConfirmedRollbackEqualsFreshIndex(t *testing.T) {
	chain := fakenode.BuildChain(0, 6, "", "a") // 0..5
	h := newHarness(t, chain, Config{Confirmations: 0, MaxConcurrency: 4, MaxReorgDepth: 10})
	if err := h.runUntilIdle(); err != nil {
		t.Fatal(err)
	}
	h.mustFinalHeights([]uint64{0, 1, 2, 3, 4, 5})

	// Fork from height 2, longer fork 3..6.
	fork := fakenode.BuildChain(3, 4, h.node.CanonicalHash(2), "b")
	h.node.SetReorg(2, fork)
	if err := h.runUntilIdle(); err != nil {
		t.Fatalf("confirmed reorg: %v", err)
	}

	// Reference state: index the resulting canonical chain from zero.
	ref := newHarness(t,
		append(fakenode.BuildChain(0, 3, "", "a"),
			fakenode.BuildChain(3, 4, "0xa-2", "b")...),
		Config{Confirmations: 0, MaxConcurrency: 4, MaxReorgDepth: 10})
	if err := ref.runUntilIdle(); err != nil {
		t.Fatal(err)
	}

	if h.st.BlockCount() != ref.st.BlockCount() {
		t.Fatalf("block count after reorg = %d, fresh = %d", h.st.BlockCount(), ref.st.BlockCount())
	}
	if h.st.EventCount() != ref.st.EventCount() {
		t.Fatalf("event count after reorg = %d, fresh = %d", h.st.EventCount(), ref.st.EventCount())
	}
	for _, height := range h.st.Heights() {
		got, _ := h.st.BlockAt(height)
		want, _ := ref.st.BlockAt(height)
		if got.Hash != want.Hash {
			t.Fatalf("height %d after reorg hash=%s, fresh-chain hash=%s",
				height, got.Hash, want.Hash)
		}
		if len(got.Events) != len(want.Events) {
			t.Fatalf("height %d event count mismatch", height)
		}
	}
	// No residue: every finalized event payload belongs to b above height 2.
	for _, height := range []uint64{3, 4, 5, 6} {
		b, _ := h.st.BlockAt(height)
		if b.Events[0].Payload != "evt-b-"+itoa(height) {
			t.Fatalf("leftover old-fork data at height %d: %s", height, b.Events[0].Payload)
		}
	}
}

// Reorg deeper than MaxReorgDepth must fail explicitly with
// ErrReorgTooDeep rather than silently truncating.
func TestReorgDeeperThanLimitFailsExplicitly(t *testing.T) {
	chain := fakenode.BuildChain(0, 6, "", "a") // 0..5
	h := newHarness(t, chain, Config{Confirmations: 0, MaxConcurrency: 2, MaxReorgDepth: 2})
	if err := h.runUntilIdle(); err != nil {
		t.Fatal(err)
	}
	// Fork anchored at height 0, replacing the entire 1..5 chain: the
	// common ancestor (0) is 5 blocks below the divergence anchor 5.
	fork := fakenode.BuildChain(1, 5, h.node.CanonicalHash(0), "b")
	h.node.SetReorg(0, fork)

	err := h.runUntilIdle()
	if err == nil {
		t.Fatal("expected ErrReorgTooDeep, got success")
	}
	if !errors.Is(err, ErrReorgTooDeep) {
		t.Fatalf("want ErrReorgTooDeep (distinguishable), got %v", err)
	}
}

// A node tip that goes backwards below the confirmed tip is rejected.
func TestNodeHeadBehindIsRejected(t *testing.T) {
	chain := fakenode.BuildChain(0, 3, "", "a") // 0..2
	h := newHarness(t, chain, Config{Confirmations: 0, MaxConcurrency: 1, MaxReorgDepth: 5})
	if err := h.runUntilIdle(); err != nil {
		t.Fatal(err)
	}
	// Serve a shorter canonical chain.
	h.node.SetReorg(0, fakenode.BuildChain(1, 1, h.node.CanonicalHash(0), "a")) // only 0..1
	_, err := h.idx.Step()
	if !errors.Is(err, ErrChainBehind) {
		t.Fatalf("want ErrChainBehind, got %v", err)
	}
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}

// Equal-length fork replacing the confirmed tip itself (tip height unchanged,
// tip hash changed) must be detected and rolled forward correctly.
func TestReorgSameHeightConfirmedTip(t *testing.T) {
	chain := fakenode.BuildChain(0, 4, "", "a") // 0..3
	h := newHarness(t, chain, Config{Confirmations: 0, MaxConcurrency: 2, MaxReorgDepth: 10})
	if err := h.runUntilIdle(); err != nil {
		t.Fatal(err)
	}

	// Replace heights 2..3 with a b-fork of equal length (tip stays at 3).
	fork := fakenode.BuildChain(2, 2, h.node.CanonicalHash(1), "b")
	h.node.SetReorg(1, fork)
	if err := h.runUntilIdle(); err != nil {
		t.Fatalf("same-height reorg: %v", err)
	}
	h.mustFinalHeights([]uint64{0, 1, 2, 3})
	b2, _ := h.st.BlockAt(2)
	b3, _ := h.st.BlockAt(3)
	if b2.Hash != "0xb-2" || b3.Hash != "0xb-3" {
		t.Fatalf("post-reorg hashes = %s,%s want 0xb-2,0xb-3", b2.Hash, b3.Hash)
	}
}
