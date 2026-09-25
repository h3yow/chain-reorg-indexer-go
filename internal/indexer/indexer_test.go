package indexer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/indexer"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

func newIndexer(t *testing.T, node *fakenode.Node, confs uint64, conc int, policy *rpc.RetryPolicy) (*indexer.Indexer, *store.Store) {
	t.Helper()
	st := store.New()
	var client rpc.Client = node
	if policy != nil {
		p := *policy
		client = rpc.NewRetryClient(node, p)
	}
	ix := indexer.New(indexer.Config{
		Client:      client,
		Store:       st,
		Rule:        types.ConfirmationRule{Confirmations: confs},
		Concurrency: conc,
	})
	ix.SetLogger(t.Logf)
	return ix, st
}

func hashes(recs []store.BlockRecord) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.Hash
	}
	return out
}

func TestRepeatPullIsIdempotent(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 5)
	node := fakenode.New()
	node.SetChain(chain...)
	ix, st := newIndexer(t, node, 0, 2, nil)

	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantEvents := st.EventCount()
	if wantEvents != 5 {
		t.Fatalf("events=%d want 5", wantEvents)
	}

	// Second run re-pulls everything (indexer does not cache): must be a
	// no-op with no duplicate records.
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if st.EventCount() != 5 {
		t.Fatalf("events after repeat=%d want 5", st.EventCount())
	}
	all := st.AllBlocks()
	if len(all) != 5 {
		t.Fatalf("blocks=%d want 5", len(all))
	}
}

func TestUnconfirmedBlocksNeverExposed(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 4) // tip = 3
	node := fakenode.New()
	node.SetChain(chain...)
	ix, st := newIndexer(t, node, 3, 2, nil)

	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	finals := st.FinalBlocks()
	if len(finals) != 1 { // only height 0: tip 3 - 0 = 3
		t.Fatalf("finalized=%d want 1 (height 0)", len(finals))
	}
	if finals[0].Number != 0 {
		t.Fatalf("finalized height=%d want 0", finals[0].Number)
	}
	// Unconfirmed blocks are still indexed internally, ready to finalize.
	if all := st.AllBlocks(); len(all) != 4 {
		t.Fatalf("stored blocks=%d want 4", len(all))
	}

	// Chain grows: previously parked blocks become finalized in order.
	node.SetChain(fakenode.BuildLinear("A", 0, 7)...) // tip = 6
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	finals = st.FinalBlocks()
	if len(finals) != 4 { // heights 0..3
		t.Fatalf("finalized=%d want 4", len(finals))
	}
}

func TestMissingDataIsRejected(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 5)
	// Simulate a node that advertises tip 4 but lacks block 2 (missing data).
	kept := chain[:2]
	kept = append(kept, chain[3:]...)
	node := fakenode.New()
	node.SetChain(kept...)
	node.SetTip(4)

	ix, _ := newIndexer(t, node, 0, 1, nil)
	err := ix.Run(context.Background())
	if !errors.Is(err, indexer.ErrMissingData) {
		t.Fatalf("err=%v want ErrMissingData", err)
	}
}

func TestRateLimitRetryThenSuccess(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 4)
	node := fakenode.New()
	node.SetChain(chain...)
	// First two block fetches fail with rate limit then timeout, third succeeds.
	node.FailNext("blk", -1, rpc.ClassRateLimited, 1)
	node.FailNext("blk", -1, rpc.ClassTimeout, 1)

	policy := rpc.RetryPolicy{
		MaxAttempts: 4,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	}
	ix, st := newIndexer(t, node, 0, 1, &policy)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatalf("should recover from retryable errors: %v", err)
	}
	if st.EventCount() != 4 {
		t.Fatalf("events=%d want 4", st.EventCount())
	}
}

func TestRateLimitExhaustedFailsExplicitly(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 3)
	node := fakenode.New()
	node.SetChain(chain...)
	node.FailAlways("blk", 1, rpc.ClassRateLimited)

	policy := rpc.RetryPolicy{
		MaxAttempts: 3,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
	}
	ix, st := newIndexer(t, node, 0, 1, &policy)
	err := ix.Run(context.Background())
	if !errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("err=%v want ErrRetryExhausted, no silent skip", err)
	}
	// Height 1 could never be fetched: nothing past it may be indexed.
	if _, ok := st.Head(); ok && st.GetBlock(1) != nil {
		t.Fatal("block 1 must not be indexed when fetch always fails")
	}
}

func TestConcurrencyCapEnforced(t *testing.T) {
	chain := fakenode.BuildLinear("A", 0, 40)
	node := fakenode.New()
	node.SetChain(chain...)
	node.SetDelay(2 * time.Millisecond)
	ix, _ := newIndexer(t, node, 0, 4, nil)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if peak := node.PeakInFlight(); peak > 4 {
		t.Fatalf("peak in flight=%d exceeds cap 4", peak)
	}
	if peak := node.PeakInFlight(); peak < 2 {
		t.Fatalf("peak in flight=%d, expected real concurrency >=2", peak)
	}
}

func TestWriteOrderIndependentOfScheduling(t *testing.T) {
	// High concurrency with delay: blocks must still be stored in height order
	// with a perfectly linked parent chain.
	chain := fakenode.BuildLinear("A", 0, 50)
	node := fakenode.New()
	node.SetChain(chain...)
	node.SetDelay(time.Millisecond)
	ix, st := newIndexer(t, node, 0, 8, nil)
	if err := ix.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	all := st.AllBlocks()
	if len(all) != 50 {
		t.Fatalf("blocks=%d want 50", len(all))
	}
	for i, b := range all {
		if b.Number != uint64(i) {
			t.Fatalf("position %d stored height %d (order broken)", i, b.Number)
		}
		if i > 0 && b.ParentHash != all[i-1].Hash {
			t.Fatalf("height %d parent linkage broken", b.Number)
		}
	}
}

// stubClient rewrites the returned number of one block to simulate a node
// serving a block at the wrong height.
type rewriteNumberClient struct {
	inner  rpc.Client
	height uint64
	to     uint64
}

func (c *rewriteNumberClient) BlockByNumber(ctx context.Context, n uint64) (*types.Block, error) {
	b, err := c.inner.BlockByNumber(ctx, n)
	if err == nil && b != nil && n == c.height {
		cp := *b
		cp.Number = c.to
		return &cp, nil
	}
	return b, err
}

func (c *rewriteNumberClient) ChainTip(ctx context.Context) (uint64, error) {
	return c.inner.ChainTip(ctx)
}

func TestHeightJumpFromNodeIsRejected(t *testing.T) {
	node := fakenode.New()
	node.SetChain(fakenode.BuildLinear("A", 0, 5)...)

	st := store.New()
	ix := indexer.New(indexer.Config{
		Client:      &rewriteNumberClient{inner: node, height: 2, to: 3},
		Store:       st,
		Rule:        types.ConfirmationRule{},
		Concurrency: 1,
	})
	ix.SetLogger(t.Logf)

	err := ix.Run(context.Background())
	if !errors.Is(err, indexer.ErrHeightJump) {
		t.Fatalf("err=%v want ErrHeightJump", err)
	}
	t.Logf("height jump rejected with distinguishable reason: %v", err)
}

func TestTipFailureExhaustedFailsExplicitly(t *testing.T) {
	node := fakenode.New()
	node.SetChain(fakenode.BuildLinear("A", 0, 3)...)
	node.FailAlways("tip", -1, rpc.ClassTemporary)

	policy := rpc.RetryPolicy{
		MaxAttempts: 2,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
	}
	ix, _ := newIndexer(t, node, 0, 1, &policy)
	err := ix.Run(context.Background())
	if !errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("err=%v want ErrRetryExhausted on tip fetch", err)
	}
}

func TestContextCancelPropagates(t *testing.T) {
	node := fakenode.New()
	node.SetChain(fakenode.BuildLinear("A", 0, 50)...)
	node.SetDelay(20 * time.Millisecond)

	st := store.New()
	ix := indexer.New(indexer.Config{
		Client:      node,
		Store:       st,
		Rule:        types.ConfirmationRule{},
		Concurrency: 2,
	})
	ix.SetLogger(t.Logf)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := ix.Run(ctx)
	if err == nil {
		t.Fatal("cancelled run must return an error, not nil")
	}
}
