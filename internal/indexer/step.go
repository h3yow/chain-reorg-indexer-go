package indexer

import (
	"errors"
	"fmt"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
)

// Step performs one incremental advancement cycle:
//
//  1. read the node tip;
//  2. fetch/validate new blocks into the unconfirmed window, reusing cached
//     unconfirmed blocks so identical data is never pulled twice;
//  3. on parent-hash discontinuity, locate the common ancestor, roll the
//     confirmed store back to it (within MaxReorgDepth), discard stale
//     unconfirmed blocks, and rebuild along the new chain;
//  4. confirm every window block buried under >= Confirmations blocks.
//
// It returns moved=true when any state changed (fetched or confirmed).
func (x *Indexer) Step() (bool, error) {
	tip, err := x.client.GetChainHead()
	if err != nil {
		return false, x.wrapRPC(err)
	}
	x.tip = &rpcHead{number: tip.Number, hash: tip.Hash}

	lastH, lastHash, hasAnchor := x.store.LastConfirmed()

	var firstH uint64
	if hasAnchor {
		firstH = lastH + 1
	} else {
		firstH = x.cfg.StartHeight
	}

	// A well-behaved node never reports a tip below our confirmed tip.
	if hasAnchor && tip.Number < lastH {
		return false, fmt.Errorf("%w: node tip %d < confirmed %d",
			ErrChainBehind, tip.Number, lastH)
	}

	// Same-height reorg: the chain did not grow but the confirmed tip block
	// itself changed hash (a fork of equal length). This cannot be detected
	// by parent chaining while extending, so it is checked explicitly.
	reorged := false
	if hasAnchor && tip.Number == lastH && tip.Hash != lastHash {
		x.logf("TIP-DIVERGENCE height=%d stored=%s node=%s reason=same-height-reorg",
			lastH, short(lastHash), short(tip.Hash))
		anchorH, anchorHash, err := x.resolveReorg(lastH, tip.Number)
		if err != nil {
			return false, err
		}
		fetched, ferr := x.fetchRange(anchorH+1, anchorH, anchorHash, true, tip.Number)
		if ferr != nil {
			return false, ferr
		}
		x.absorb(fetched)
		reorged = true
	}

	// On cold start firstH is StartHeight; after a same-height rollback the
	// old firstH (=lastH+1) is above the tip, so recompute from the store.
	effectiveFirstH := firstH
	if !hasAnchor || reorged {
		if h, _, ok := x.store.LastConfirmed(); ok {
			effectiveFirstH = h + 1
		} else {
			effectiveFirstH = x.cfg.StartHeight
		}
	}

	if effectiveFirstH > tip.Number {
		return reorged, nil // chain shorter than the next required height
	}

	if !reorged {
		if _, err := x.advanceWindow(effectiveFirstH, lastH, lastHash, hasAnchor, tip.Number); err != nil {
			return false, err
		}
	}
	moved := reorged

	// Recompute the confirmation start AFTER a possible rollback: the store
	// tip may have moved backwards to the common ancestor.
	confirmFrom := effectiveFirstH
	if h, _, ok := x.store.LastConfirmed(); ok {
		confirmFrom = h + 1
	}

	// A block is final when h + Confirmations <= tip.Number.
	var confirmUpTo uint64
	if tip.Number >= uint64(x.cfg.Confirmations) {
		confirmUpTo = tip.Number - uint64(x.cfg.Confirmations)
	}
	for h := confirmFrom; h <= confirmUpTo; h++ {
		b := x.pending[h]
		if b == nil {
			return moved, fmt.Errorf("%w: block %d missing while confirming", ErrHeightGap, h)
		}
		if err := x.store.AppendConfirmed(b); err != nil {
			return moved, x.mapStoreError(err, b)
		}
		delete(x.pending, h)
		moved = true
		x.logf("CONFIRM height=%d hash=%s parent=%s events=%d reason=buried tip=%d conf=%d",
			b.Number, short(b.Hash), short(b.ParentHash), len(b.Events), tip.Number, x.cfg.Confirmations)
	}
	return moved, nil
}

// advanceWindow fetches/validates blocks from..to (inclusive), reusing
// cached pending blocks. anchorH/anchorHash identify the block immediately
// before `from` for parent-hash chaining (hasAnchor=false on cold start).
// On parent mismatch it performs exactly one rollback cycle and refetches.
func (x *Indexer) advanceWindow(from, anchorH uint64, anchorHash string, hasAnchor bool, to uint64) (bool, error) {
	// Cached unconfirmed blocks may sit on a dead fork. Verify each cached
	// height against the node's current canonical hash and invalidate from
	// the first divergence upward; fetchRange then re-pulls them.
	if err := x.invalidatePending(from, to); err != nil {
		return false, err
	}

	fetched, err := x.fetchRange(from, anchorH, anchorHash, hasAnchor, to)
	if err == nil {
		x.absorb(fetched)
		return len(fetched) > 0, nil
	}
	if !errors.Is(err, ErrParentHashMismatch) {
		return false, err
	}

	// --- Reorg resolution ---
	newAnchorH, newAnchorHash, rerr := x.resolveReorg(anchorH, to)
	if rerr != nil {
		return false, rerr
	}

	fetched, err = x.fetchRange(newAnchorH+1, newAnchorH, newAnchorHash, true, to)
	if err != nil {
		return false, err
	}
	x.absorb(fetched)
	return true, nil
}

// resolveReorg finds the common ancestor below or at startH, rolls the
// confirmed store back to it, and drops stale unconfirmed window blocks up
// to the current node tip. Returns the new anchor.
func (x *Indexer) resolveReorg(startH, tipHeight uint64) (uint64, string, error) {
	anchorH, anchorHash, err := x.findCommonAncestor(startH, "")
	if err != nil {
		return 0, "", err
	}
	if err := x.store.RollbackTo(anchorH); err != nil {
		return 0, "", err
	}
	for h := range x.pending {
		if h > anchorH && h <= tipHeight {
			delete(x.pending, h)
		}
	}
	x.logf("ROLLBACK to=%d hash=%s reason=parent-hash-discontinuity", anchorH, short(anchorHash))
	return anchorH, anchorHash, nil
}

// absorb merges freshly fetched blocks into the pending cache.
func (x *Indexer) absorb(fetched map[uint64]*model.Block) {
	for h, b := range fetched {
		x.pending[h] = b
	}
}

func (x *Indexer) mapStoreError(err error, b *model.Block) error {
	switch {
	case errors.Is(err, store.ErrGap):
		return fmt.Errorf("%w: %v", ErrHeightGap, err)
	case errors.Is(err, store.ErrParentMismatch):
		return fmt.Errorf("%w: %v", ErrParentHashMismatch, err)
	default:
		return err
	}
}

func (x *Indexer) wrapRPC(err error) error {
	switch {
	case errors.Is(err, rpc.ErrTimeout),
		errors.Is(err, rpc.ErrTemporary),
		errors.Is(err, rpc.ErrRateLimited):
		// %w preserves the full error chain so callers can errors.Is both
		// ErrRetryExhausted and the underlying rpc sentinel.
		return fmt.Errorf("%w: %w", ErrRetryExhausted, err)
	default:
		return err
	}
}

func (x *Indexer) logf(format string, args ...any) {
	if x.log != nil {
		x.log.Printf(format, args...)
	}
}

func short(s string) string {
	if len(s) <= 10 {
		return s
	}
	return s[:10]
}
