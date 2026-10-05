package indexer_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/indexer"
	"github.com/h3yow/chain-reorg-indexer-go/internal/jsonrpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// newHTTPIndexer builds an indexer whose client talks to node over HTTP
// JSON-RPC (the same code path used against a real remote node).
func newHTTPIndexer(t *testing.T, node *fakenode.Node, confs uint64, conc int) (*indexer.Indexer, *store.Store) {
	t.Helper()
	srv := httptest.NewServer(jsonrpc.NewHandler(node))
	t.Cleanup(srv.Close)
	client := rpc.NewRetryClient(jsonrpc.NewHTTPClient(srv.URL), rpc.RetryPolicy{
		MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
	})
	st := store.New()
	ix := indexer.New(indexer.Config{
		Client:      client,
		Store:       st,
		Rule:        types.ConfirmationRule{Confirmations: confs},
		Concurrency: conc,
	})
	ix.SetLogger(t.Logf)
	return ix, st
}

// assertStoreEqualsReference verifies the store matches a fresh from-genesis
// index of chain: identical hashes, finalized flags and event sets — i.e.
// "incremental result == indexing the node's current chain from scratch".
func assertStoreEqualsReference(t *testing.T, st *store.Store, chain []*types.Block, confs uint64) {
	t.Helper()
	ref := referenceIndex(t, chain, confs)
	got, want := st.AllBlocks(), ref.AllBlocks()
	if len(got) != len(want) {
		t.Fatalf("blocks=%d reference=%d", len(got), len(want))
	}
	for i := range got {
		if got[i].Hash != want[i].Hash {
			t.Fatalf("height %d: hash %s != reference %s", got[i].Number, got[i].Hash, want[i].Hash)
		}
		if got[i].Finalized != want[i].Finalized {
			t.Fatalf("height %d: finalized=%v reference=%v", got[i].Number, got[i].Finalized, want[i].Finalized)
		}
	}
	if st.EventCount() != ref.EventCount() {
		t.Fatalf("events=%d reference=%d", st.EventCount(), ref.EventCount())
	}
}

// assertNoResidue verifies no block hash or event of the discarded branch
// remains anywhere in the store.
func assertNoResidue(t *testing.T, st *store.Store, discarded []*types.Block) {
	t.Helper()
	for _, b := range discarded {
		if rec := st.GetBlock(b.Number); rec != nil && rec.Hash == b.Hash {
			t.Fatalf("discarded branch residue at height %d hash %s", b.Number, b.Hash)
		}
		for _, e := range b.Events {
			if st.HasEvent(e.Key()) {
				t.Fatalf("discarded branch event remains: %s", e.Key())
			}
		}
	}
}

func TestHTTP_EndToEnd_EquivalentToInProcess(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 12)

	// Index the same chain twice: once over HTTP JSON-RPC, once in-process.
	httpNode := fakenode.New()
	httpNode.SetChain(chain...)
	ixHTTP, stHTTP := newHTTPIndexer(t, httpNode, 3, 4)
	if err := ixHTTP.Run(context.Background()); err != nil {
		t.Fatalf("http run: %v", err)
	}

	directNode := fakenode.New()
	directNode.SetChain(chain...)
	ixDirect, stDirect := newIndexer(t, directNode, 3, 4, nil)
	if err := ixDirect.Run(context.Background()); err != nil {
		t.Fatalf("direct run: %v", err)
	}

	got, want := hashes(stHTTP.AllBlocks()), hashes(stDirect.AllBlocks())
	if len(got) != len(want) {
		t.Fatalf("http blocks=%d direct=%d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("height %d: http hash %s != direct %s", i, got[i], want[i])
		}
	}
	if stHTTP.EventCount() != stDirect.EventCount() {
		t.Fatalf("events: http=%d direct=%d", stHTTP.EventCount(), stDirect.EventCount())
	}
	if len(stHTTP.FinalBlocks()) != len(stDirect.FinalBlocks()) {
		t.Fatalf("finalized: http=%d direct=%d", len(stHTTP.FinalBlocks()), len(stDirect.FinalBlocks()))
	}
	t.Logf("scenario=http-e2e blocks=%d finalized=%d events=%d identical to in-process run",
		len(got), len(stHTTP.FinalBlocks()), stHTTP.EventCount())
}

func TestHTTP_Reorg_ConvergesToNodeChain(t *testing.T) {
	oldChain := fakenode.BuildLinear("A", 0, 10)
	fork := fakenode.BuildFork("A", 6, 6) // heights 6..11
	newChain := append(append([]*types.Block{}, oldChain[:6]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newHTTPIndexer(t, node, 4, 3)

	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("initial run: %v", err)
	}
	node.ApplyReorg(6, fork...)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run after reorg: %v", err)
	}

	assertStoreEqualsReference(t, st, newChain, 4)
	assertNoResidue(t, st, oldChain[6:])
	t.Logf("scenario=http-reorg fork-at=6 new-tip=11 result equals fresh index of new chain, no residue")
}

func TestHTTP_RateLimitExhausted_FailsWithoutSkippingHeights(t *testing.T) {
	node := fakenode.New()
	node.SetChain(fakenode.BuildLinear("A", 0, 6)...)
	// Every block fetch is rate limited forever; retries (3 attempts) exhaust.
	node.FailAlways("blk", -1, rpc.ClassRateLimited)

	ix, st := newHTTPIndexer(t, node, 2, 2)
	err := ix.Run(context.Background())
	if !errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("want ErrRetryExhausted, got %v", err)
	}
	// Heights before the failing one were written contiguously; nothing was
	// skipped and the run reported failure instead of silently advancing.
	if head, ok := st.Head(); ok {
		for h := uint64(0); h <= head; h++ {
			if st.GetBlock(h) == nil {
				t.Fatalf("gap at height %d below head %d: a height was skipped", h, head)
			}
		}
		t.Logf("scenario=http-429-exhausted err=%v stored-head=%d contiguous (no skipped heights)", err, head)
	} else {
		t.Logf("scenario=http-429-exhausted err=%v store empty, no height skipped", err)
	}
}

func TestReorg_ZeroConfirmations_SameHeightSwapCorrected(t *testing.T) {
	// With Confirmations=0 every block is immediately finalized/presented.
	oldChain := fakenode.BuildLinear("A", 0, 8) // heights 0..7, all presented
	fork := fakenode.BuildFork("A", 3, 5)     // same heights 3..7, new hashes, tip unchanged
	newChain := append(append([]*types.Block{}, oldChain[:3]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 0, 2, nil)

	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("initial run: %v", err)
	}
	// Everything was already handed out as a final result.
	if got := len(st.FinalBlocks()); got != 8 {
		t.Fatalf("finalized before reorg=%d want 8", got)
	}
	presented := hashes(st.AllBlocks())

	// Node swaps hashes at heights 3..7; tip stays 7.
	node.ApplyReorg(3, fork...)
	if node.Tip() != 7 {
		t.Fatalf("tip moved: %d", node.Tip())
	}

	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run after same-height swap: %v", err)
	}

	assertStoreEqualsReference(t, st, newChain, 0)
	assertNoResidue(t, st, oldChain[3:])

	// The corrected hashes must differ from what was presented earlier.
	for i, h := range hashes(st.AllBlocks()) {
		if i >= 3 && h == presented[i] {
			t.Fatalf("height %d still holds the replaced hash %s", i, h)
		}
	}
	t.Logf("scenario=zero-conf-same-height-swap heights=3..7 tip-unchanged corrected to node chain")
}

func TestReorg_ZeroConfirmations_DeepSwapBelowPresentedHead(t *testing.T) {
	// Same-height swap that reaches below everything already presented:
	// with confirmations=0 this must be corrected (no finality protection),
	// whereas with confirmations>0 the same shape is ErrFinalityReorg.
	oldChain := fakenode.BuildLinear("A", 0, 6)
	fork := fakenode.BuildFork("A", 1, 5) // replace 1..5, tip unchanged at 5
	newChain := append(append([]*types.Block{}, oldChain[:1]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 0, 1, nil)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("initial run: %v", err)
	}
	if got := len(st.FinalBlocks()); got != 6 {
		t.Fatalf("finalized before reorg=%d want 6", got)
	}

	node.ApplyReorg(1, fork...)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run after deep swap: %v", err)
	}
	assertStoreEqualsReference(t, st, newChain, 0)
	assertNoResidue(t, st, oldChain[1:])
	t.Logf("scenario=zero-conf-deep-swap fork-at=1 below presented head corrected (protection disabled at 0 confs)")
}

func TestReorg_PositiveConfirmations_FinalityProtectionIntact(t *testing.T) {
	// Sanity anchor: with confirmations>0 a reorg that would revert finalized
	// blocks still fails explicitly and leaves finalized data untouched.
	oldChain := fakenode.BuildLinear("A", 0, 10)
	fork := fakenode.BuildFork("A", 2, 9) // fork below the finalized head

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newIndexer(t, node, 3, 2, nil)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("initial run: %v", err)
	}
	finalizedBefore := hashes(st.FinalBlocks())
	if len(finalizedBefore) == 0 {
		t.Fatalf("expected finalized blocks before reorg")
	}

	node.ApplyReorg(2, fork...)
	err := ix.Run(context.Background())
	if !errors.Is(err, indexer.ErrFinalityReorg) {
		t.Fatalf("want ErrFinalityReorg, got %v", err)
	}
	if got := hashes(st.FinalBlocks()); len(got) != len(finalizedBefore) {
		t.Fatalf("finalized set changed after rejected reorg: %d -> %d", len(finalizedBefore), len(got))
	} else {
		for i := range got {
			if got[i] != finalizedBefore[i] {
				t.Fatalf("finalized height %d hash changed after rejected reorg", i)
			}
		}
	}
	t.Logf("scenario=positive-conf-finality-protection finalized=%d preserved, err=%v",
		len(finalizedBefore), err)
}
