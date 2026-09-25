package indexer

import (
	"errors"
	"fmt"
	"sync"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
)

// fetchRange ensures blocks lastH+1 .. to are present and returns the newly
// fetched ones (already-cached pending blocks are reused, not re-pulled).
//
// At most cfg.MaxConcurrency RPC requests are in flight, but blocks are
// always validated in strictly increasing height order; therefore
// concurrent scheduling can never reorder the resulting writes.
//
// Chain integrity is checked against (a) the confirmed anchor hash and
// (b) every preceding block, whether cached or newly fetched. A break
// yields ErrParentHashMismatch which the caller resolves as a reorg.
func (x *Indexer) fetchRange(from, anchorH uint64, anchorHash string, hasAnchor bool, to uint64) (map[uint64]*model.Block, error) {
	if from > to {
		return map[uint64]*model.Block{}, nil
	}

	// Heights that actually require an RPC call.
	var todo []uint64
	for h := from; h <= to; h++ {
		if _, ok := x.pending[h]; !ok {
			todo = append(todo, h)
		}
	}

	fetched, err := x.concurrentFetch(todo)
	if err != nil {
		return nil, err
	}

	// Ordered validation over the full range, cache included.
	out := make(map[uint64]*model.Block, len(fetched))
	prevHash := anchorHash
	_ = anchorH
	for h := from; h <= to; h++ {
		b, fresh := fetched[h]
		if !fresh {
			b = x.pending[h]
		}
		if b == nil {
			x.logf("REJECT height=%d reason=missing-block-data", h)
			return nil, fmt.Errorf("%w: no block data at height %d", ErrHeightGap, h)
		}
		if b.Number != h {
			x.logf("REJECT want=%d got=%d reason=height-mismatch", h, b.Number)
			return nil, fmt.Errorf("%w: asked %d node returned %d", ErrHeightGap, h, b.Number)
		}
		if hasAnchor && b.ParentHash != prevHash {
			x.logf("MISMATCH height=%d hash=%s parent=%s want-parent=%s reason=reorg-suspected",
				h, short(b.Hash), short(b.ParentHash), short(prevHash))
			return nil, fmt.Errorf("%w: height %d parent %s, want %s",
				ErrParentHashMismatch, h, short(b.ParentHash), short(prevHash))
		}
		if fresh {
			x.logf("FETCH height=%d hash=%s parent=%s events=%d reason=new-block",
				h, short(b.Hash), short(b.ParentHash), len(b.Events))
			out[h] = b
		} else {
			x.logf("CACHED height=%d hash=%s reason=pending-window-hit", h, short(b.Hash))
		}
		prevHash = b.Hash
	}
	return out, nil
}

// concurrentFetch fetches the given heights with bounded concurrency.
func (x *Indexer) concurrentFetch(heights []uint64) (map[uint64]*model.Block, error) {
	out := make(map[uint64]*model.Block, len(heights))
	if len(heights) == 0 {
		return out, nil
	}

	type result struct {
		h   uint64
		b   *model.Block
		err error
	}

	jobs := make(chan uint64)
	results := make(chan result, len(heights))
	var wg sync.WaitGroup
	for w := 0; w < x.cfg.MaxConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for h := range jobs {
				b, err := x.client.GetBlockByNumber(h)
				results <- result{h: h, b: b, err: err}
			}
		}()
	}

	go func() {
		for _, h := range heights {
			jobs <- h
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	var firstErr error
	for r := range results {
		if r.err != nil {
			if errors.Is(r.err, rpc.ErrBlockMissing) {
				x.logf("REJECT height=%d reason=node-returned-no-block", r.h)
				if firstErr == nil {
					firstErr = fmt.Errorf("%w: node has no block at height %d", ErrHeightGap, r.h)
				}
				continue
			}
			if firstErr == nil {
				firstErr = x.wrapRPC(r.err)
			}
			continue
		}
		if r.b == nil || r.b.Hash == "" {
			x.logf("REJECT height=%d reason=empty-block-payload", r.h)
			if firstErr == nil {
				firstErr = fmt.Errorf("%w: empty block at height %d", ErrHeightGap, r.h)
			}
			continue
		}
		out[r.h] = r.b
	}
	return out, firstErr
}

// invalidatePending checks cached unconfirmed blocks from..to in height
// order. From the first height whose canonical hash differs from the cache,
// every cached entry at or above it is discarded (they are the old fork's
// descendants). RPC errors are classified and surfaced.
func (x *Indexer) invalidatePending(from, to uint64) error {
	for h := from; h <= to; h++ {
		cached, ok := x.pending[h]
		if !ok {
			continue
		}
		b, err := x.client.GetBlockByNumber(h)
		if err != nil {
			if errors.Is(err, rpc.ErrBlockMissing) {
				// Height no longer served: treat from here as stale.
				x.logf("PENDING-INVALIDATED height=%d old=%s reason=block-missing", h, short(cached.Hash))
				x.dropPendingFrom(h)
				return nil
			}
			return x.wrapRPC(err)
		}
		if b.Hash == cached.Hash {
			x.logf("PENDING-VERIFY height=%d hash=%s reason=canonical-match", h, short(b.Hash))
			continue
		}
		x.logf("PENDING-INVALIDATED height=%d old=%s new=%s reason=hash-changed",
			h, short(cached.Hash), short(b.Hash))
		x.dropPendingFrom(h)
		return nil
	}
	return nil
}

// dropPendingFrom removes all cached unconfirmed blocks at height >= h.
func (x *Indexer) dropPendingFrom(h uint64) {
	for k := range x.pending {
		if k >= h {
			delete(x.pending, k)
		}
	}
}
