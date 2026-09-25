package indexer

import (
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
)

// With a high fetch concurrency, finalized records must still be a
// gap-free, parent-connected 0..N chain regardless of worker scheduling.
// Run repeatedly under -race to surface any ordering/race regression.
func TestHighConcurrencyProducesOrderedWrites(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		chain := fakenode.BuildChain(0, 12, "", "a") // 0..11
		h := newHarness(t, chain, Config{
			Confirmations: 0, MaxConcurrency: 8, MaxReorgDepth: 5,
		})
		if err := h.runUntilIdle(); err != nil {
			t.Fatalf("iter %d: %v", iter, err)
		}
		heights := h.st.Heights()
		if len(heights) != 12 {
			t.Fatalf("iter %d: heights=%v", iter, heights)
		}
		for i, want := range heights {
			if uint64(i) != want {
				t.Fatalf("iter %d: write order broken at %d: heights=%v", iter, i, heights)
			}
			b, _ := h.st.BlockAt(want)
			if i > 0 && b.ParentHash != h.stHashAt(uint64(i-1)) {
				t.Fatalf("iter %d: parent chain broken at %d", iter, i)
			}
		}
	}
}
