package indexer

import (
	"errors"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
)

// Rate limiting on block fetch: indexer must survive temporary 429s via the
// client retry budget and finish with the correct final state.
func TestIndexerSurvivesRateLimitThenCompletes(t *testing.T) {
	chain := fakenode.BuildChain(0, 4, "", "a")
	h := newHarness(t, chain, Config{Confirmations: 0, MaxConcurrency: 1, MaxReorgDepth: 5})

	// First two block fetches are rate limited; retries recover.
	h.node.SetFailure("eth_getBlockByNumber", fakenode.FailRateLimit, 2)
	if err := h.runUntilIdle(); err != nil {
		t.Fatalf("index should survive rate limits via retries: %v", err)
	}
	h.mustFinalHeights([]uint64{0, 1, 2, 3})
}

// Persistent rate limiting beyond the retry budget must fail explicitly with
// a retry-exhausted error that still carries the distinguishable rpc cause.
func TestIndexerFailsExplicitlyWhenRateLimitPersists(t *testing.T) {
	chain := fakenode.BuildChain(0, 4, "", "a")
	h := newHarness(t, chain, Config{Confirmations: 0, MaxConcurrency: 1, MaxReorgDepth: 5})

	h.node.SetFailure("eth_getBlockByNumber", fakenode.FailTemporary, 100)
	err := h.runUntilIdle()
	if err == nil {
		t.Fatal("expected explicit failure after persistent node errors")
	}
	if !errors.Is(err, ErrRetryExhausted) {
		t.Fatalf("want ErrRetryExhausted, got %v", err)
	}
	if !errors.Is(err, rpc.ErrTemporary) {
		t.Fatalf("exhausted error must preserve the underlying cause, got %v", err)
	}
	// Nothing must have been partially confirmed.
	if h.st.BlockCount() != 0 {
		t.Fatalf("store must stay empty after failed run, got %d blocks", h.st.BlockCount())
	}
}

// Temporary failures + timeouts on chain head are classified distinctly and
// eventually succeed; the final state is identical to a clean run.
func TestIndexerHeadTemporaryThenOk(t *testing.T) {
	chain := fakenode.BuildChain(0, 3, "", "a")
	h := newHarness(t, chain, Config{Confirmations: 1, MaxConcurrency: 2, MaxReorgDepth: 5})
	h.node.SetFailure("eth_getChainHead", fakenode.FailTemporary, 2)
	if err := h.runUntilIdle(); err != nil {
		t.Fatalf("temporary head failures should be retried: %v", err)
	}
	h.mustFinalHeights([]uint64{0, 1})
}

// Node-induced timeout is surfaced as retry-exhausted wrapping the distinct
// ErrTimeout cause (not ErrTemporary / ErrRateLimited).
func TestIndexerTimeoutCauseIsDistinguishable(t *testing.T) {
	chain := fakenode.BuildChain(0, 2, "", "a")
	n := fakenode.New(chain)
	c := rpc.NewHTTPClient(rpc.Config{
		Endpoint: n.URL(), TimeoutMillis: 100, MaxRetries: 0, BackoffMillis: 1,
	})
	defer c.Close()
	defer n.Close()
	n.SetFailure("eth_getChainHead", fakenode.FailTimeout, 1)
	n.SetLatencyMillis(300)

	st := store.NewMemory()
	idx := New(c, st, Config{Confirmations: 0, MaxConcurrency: 1, MaxReorgDepth: 5}, testLogger{t})
	_, err := idx.Step()
	if err == nil {
		t.Fatal("expected timeout failure")
	}
	if !errors.Is(err, ErrRetryExhausted) || !errors.Is(err, rpc.ErrTimeout) {
		t.Fatalf("want exhausted+ErrTimeout, got %v", err)
	}
	if errors.Is(err, rpc.ErrRateLimited) || errors.Is(err, rpc.ErrTemporary) {
		t.Fatalf("timeout cause must stay distinct, got %v", err)
	}
}
