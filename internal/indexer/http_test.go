package indexer_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/indexer"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// newHTTPIndexer serves node over HTTP JSON-RPC and builds an indexer that
// talks to it through rpc.JSONRPCClient (wrapped in a RetryClient).
func newHTTPIndexer(t *testing.T, node *fakenode.Node, confs uint64, conc int) (*indexer.Indexer, *store.Store) {
	t.Helper()
	srv := httptest.NewServer(node.Handler())
	t.Cleanup(srv.Close)
	inner := rpc.NewJSONRPCClient(srv.URL, 2*time.Second)
	client := rpc.NewRetryClient(inner, rpc.RetryPolicy{
		MaxAttempts: 4, BaseBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond,
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

func assertSameStore(t *testing.T, got *store.Store, want *store.Store) {
	t.Helper()
	g, w := got.AllBlocks(), want.AllBlocks()
	if len(g) != len(w) {
		t.Fatalf("blocks=%d want %d", len(g), len(w))
	}
	for i := range g {
		if g[i].Hash != w[i].Hash || g[i].Finalized != w[i].Finalized {
			t.Fatalf("height %d mismatch: got (%s,fin=%v) want (%s,fin=%v)",
				g[i].Number, g[i].Hash, g[i].Finalized, w[i].Hash, w[i].Finalized)
		}
	}
	if got.EventCount() != want.EventCount() {
		t.Fatalf("events=%d want %d", got.EventCount(), want.EventCount())
	}
}

func TestHTTP_SuccessPathMatchesInProcess(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 10)

	httpNode := fakenode.New()
	httpNode.SetChain(chain...)
	ix, st := newHTTPIndexer(t, httpNode, 3, 3)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("http run: %v", err)
	}

	// The same chain indexed in-process must produce identical results.
	ref := referenceIndex(t, chain, 3)
	assertSameStore(t, st, ref)
	t.Logf("scenario=http-success blocks=%d finalized=%d events=%d (identical to in-process)",
		len(st.AllBlocks()), len(st.FinalBlocks()), st.EventCount())
}

func TestHTTP_RateLimitRetriedThenSucceeds(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 6)
	node := fakenode.New()
	node.SetChain(chain...)
	node.FailNext("blk", -1, rpc.ClassRateLimited, 3) // server answers 429 x3

	ix, st := newHTTPIndexer(t, node, 2, 2)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run with injected 429s: %v", err)
	}
	ref := referenceIndex(t, chain, 2)
	assertSameStore(t, st, ref)
	t.Logf("scenario=http-rate-limit-retried blocks=%d", len(st.AllBlocks()))
}

func TestHTTP_RetryExhaustedSurfaces(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 4)
	node := fakenode.New()
	node.SetChain(chain...)
	node.FailAlways("tip", -1, rpc.ClassTimeout) // server answers 408 forever

	ix, _ := newHTTPIndexer(t, node, 2, 1)
	err := ix.Run(context.Background())
	if !errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("err=%v want ErrRetryExhausted", err)
	}
	t.Logf("scenario=http-retry-exhausted err=%v", err)
}

func TestHTTP_PermanentErrorFailsImmediately(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 4)
	node := fakenode.New()
	node.SetChain(chain...)
	node.FailAlways("blk", 2, rpc.ClassPermanent) // JSON-RPC error -32800

	ix, _ := newHTTPIndexer(t, node, 2, 1)
	err := ix.Run(context.Background())
	if err == nil || errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("permanent node error must fail immediately, got %v", err)
	}
	if rpc.ClassOf(errors.Unwrap(err)) == rpc.ClassUnknown {
		t.Logf("note: wrapped error chain: %v", err)
	}
	// Exactly one node call for the failing height: no retries on permanent.
	count := 0
	for _, c := range node.Calls() {
		if c.Method == "blk" && c.Number == 2 {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("node calls for height 2 = %d, want 1 (no retry on permanent)", count)
	}
	t.Logf("scenario=http-permanent-no-retry calls-for-height-2=%d err=%v", count, err)
}

func TestHTTP_SameHeightHashSwapCorrected(t *testing.T) {
	oldChain := fakenode.BuildLinear("A", 0, 6) // 0..5
	fork := fakenode.BuildFork("A", 4, 2)       // 4..5, tip unchanged
	newChain := append(append([]*types.Block{}, oldChain[:4]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newHTTPIndexer(t, node, 3, 2) // finalized 0..2, tail 3..5 mutable
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	node.ApplyReorg(4, fork...) // same tip, different hashes at 4..5
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run after same-height swap over http: %v", err)
	}

	ref := referenceIndex(t, newChain, 3)
	assertSameStore(t, st, ref)
	for _, b := range oldChain[4:] {
		if st.HasEvent(b.Events[0].Key()) {
			t.Fatalf("replaced branch event remains: %s", b.Events[0].Key())
		}
	}
	t.Logf("scenario=http-same-height-hash-swap corrected blocks=%d", len(st.AllBlocks()))
}

func TestHTTP_ZeroConfirmationsReorgCorrected(t *testing.T) {
	oldChain := fakenode.BuildLinear("A", 0, 6)
	fork := fakenode.BuildFork("A", 4, 2)
	newChain := append(append([]*types.Block{}, oldChain[:4]...), fork...)

	node := fakenode.New()
	node.SetChain(oldChain...)
	ix, st := newHTTPIndexer(t, node, 0, 2) // zero confirmations over HTTP
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(st.FinalBlocks()); got != 6 {
		t.Fatalf("finalized=%d want 6 (zero confirmations)", got)
	}

	node.ApplyReorg(4, fork...)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("run after reorg with zero confirmations over http: %v", err)
	}

	ref := referenceIndex(t, newChain, 0)
	assertSameStore(t, st, ref)
	for _, b := range oldChain[4:] {
		if st.HasEvent(b.Events[0].Key()) {
			t.Fatalf("replaced branch event remains: %s", b.Events[0].Key())
		}
	}
	t.Logf("scenario=http-zero-confirmations-reorg corrected blocks=%d", len(st.AllBlocks()))
}
