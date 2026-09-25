package indexer

import (
	"errors"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
)

// Confirmations=2 on a 0..4 chain: only 0..2 may be presented as final;
// 3,4 stay unconfirmed and must never appear in the store.
func TestConfirmationPolicyHidesUnfinalizedTip(t *testing.T) {
	chain := fakenode.BuildChain(0, 5, "", "a") // heights 0..4
	h := newHarness(t, chain, Config{Confirmations: 2, MaxConcurrency: 3, MaxReorgDepth: 10})
	if err := h.runUntilIdle(); err != nil {
		t.Fatal(err)
	}
	h.mustFinalHeights([]uint64{0, 1, 2})
	if h.st.EventCount() != 3 {
		t.Fatalf("event count = %d, want 3 (unconfirmed events must not be exposed)", h.st.EventCount())
	}

	// Chain grows by one block: block 3 becomes final.
	h.node.Extend(fakenode.BuildChain(5, 1, "0xa-4", "a")) // add height 5
	if err := h.runUntilIdle(); err != nil {
		t.Fatal(err)
	}
	h.mustFinalHeights([]uint64{0, 1, 2, 3})
}

// Repeated pulls of the same height (additional idle cycles) must not create
// duplicate records or additional events.
func TestRepeatedPullsDeduplicate(t *testing.T) {
	chain := fakenode.BuildChain(0, 4, "", "a")
	h := newHarness(t, chain, Config{Confirmations: 1, MaxConcurrency: 2, MaxReorgDepth: 10})
	if err := h.runUntilIdle(); err != nil {
		t.Fatal(err)
	}
	blocksAfterFirst := h.st.BlockCount()
	eventsAfterFirst := h.st.EventCount()

	// Many extra cycles against an unchanged chain: pending cache hits and
	// idempotent confirms must leave the store untouched.
	for i := 0; i < 5; i++ {
		moved, err := h.idx.Step()
		if err != nil {
			t.Fatalf("idle cycle %d: %v", i, err)
		}
		if moved && i > 0 {
			t.Fatalf("cycle %d reported movement with nothing new", i)
		}
	}
	if h.st.BlockCount() != blocksAfterFirst {
		t.Fatalf("block count changed on repeated pulls: %d -> %d",
			blocksAfterFirst, h.st.BlockCount())
	}
	if h.st.EventCount() != eventsAfterFirst {
		t.Fatalf("event count changed on repeated pulls: %d -> %d",
			eventsAfterFirst, h.st.EventCount())
	}
	h.mustFinalHeights([]uint64{0, 1, 2})
}

// A node that returns no block for a required height (height gap / missing
// data) must be rejected with the distinguishable ErrHeightGap reason.
func TestMissingBlockRejectedWithReason(t *testing.T) {
	chain := fakenode.BuildChain(0, 4, "", "a")
	h := newHarness(t, chain, Config{Confirmations: 0, MaxConcurrency: 1, MaxReorgDepth: 10})

	// After the store has block 0, make block 1 disappear permanently.
	h.node.SetFailure("eth_getBlockByNumber", fakenode.FailNotFound, 100)
	if err := h.runUntilIdle(); err == nil {
		t.Fatal("expected explicit rejection for missing block")
	} else if !errors.Is(err, ErrHeightGap) {
		t.Fatalf("want ErrHeightGap (distinguishable), got %v", err)
	}
}

// A node that serves the wrong block number for a requested height (height
// jump / inconsistent numbering) must be rejected with ErrHeightGap and must
// not be confused with a parent-hash reorg.
func TestHeightSkipRejectedWithDistinguishableReason(t *testing.T) {
	chain := fakenode.BuildChain(0, 5, "", "a")
	h := newHarness(t, chain, Config{Confirmations: 0, MaxConcurrency: 1, MaxReorgDepth: 10})

	// Schedule a "serve next height" response for exactly one block call at
	// height 2: after blocks 0 and 1 are confirmed, height 2 returns block 3.
	h.node.SetFailure("eth_getBlockByNumber", fakenode.FailureMode(fakenode.FailureSkipHeight), 1)
	err := h.runUntilIdle()
	if err == nil {
		t.Fatal("expected rejection for height skip")
	}
	if !errors.Is(err, ErrHeightGap) {
		t.Fatalf("want ErrHeightGap for height skip, got %v", err)
	}
	if errors.Is(err, ErrParentHashMismatch) {
		t.Fatalf("height skip must not be classified as parent mismatch: %v", err)
	}
}
