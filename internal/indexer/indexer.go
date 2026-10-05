// Package indexer incrementally indexes blocks/events from an rpc.Client,
// honoring confirmation rules, reorgs, retries and concurrency limits.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// batchWindow bounds how far ahead a single concurrent fetch batch reaches.
const batchWindow = 256

// defaultMaxRounds bounds reorg-resolution rounds before explicit failure.
const defaultMaxRounds = 16

// Config configures an Indexer.
type Config struct {
	Client      rpc.Client
	Store       *store.Store
	Rule        types.ConfirmationRule
	Concurrency int
	// MaxRounds bounds consecutive reorg rounds; 0 => defaultMaxRounds.
	MaxRounds int
}

// Indexer performs incremental indexing runs.
type Indexer struct {
	cfg Config
	log func(format string, args ...any)
}

// New constructs an Indexer.
func New(cfg Config) *Indexer {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.MaxRounds <= 0 {
		cfg.MaxRounds = defaultMaxRounds
	}
	if cfg.Store == nil {
		cfg.Store = store.New()
	}
	return &Indexer{cfg: cfg, log: func(string, ...any) {}}
}

// SetLogger installs a printf-style logger used to print processed heights,
// block hashes and decision reasons.
func (ix *Indexer) SetLogger(f func(format string, args ...any)) {
	if f != nil {
		ix.log = f
	}
}

type fetchResult struct {
	block *types.Block
	err   error
	skip  bool // never fetched because another fetch failed first
}

// errSkipped marks a slot that was never fetched after a sibling failed.
var errSkipped = errors.New("indexer: fetch skipped due to earlier failure")

// Run performs one incremental indexing pass from the current store head up to
// the node tip, finalizing blocks that satisfy the confirmation rule.
func (ix *Indexer) Run(ctx context.Context) error {
	rounds := 0
	for {
		tip, err := ix.cfg.Client.ChainTip(ctx)
		if err != nil {
			return fmt.Errorf("indexer: fetch chain tip: %w", err)
		}
		ix.log("tip=%d confirmations-required=%d", tip, ix.cfg.Rule.Confirmations)

		head, has := ix.cfg.Store.Head()

		// Node tip fell behind our head: a branch was abandoned. Walk back to a
		// common ancestor from the node's current tip.
		if has && tip < head {
			rounds++
			if rounds > ix.cfg.MaxRounds {
				return ErrReorgNotConverging
			}
			// Probe from the stored head down; the node side is bounded by tip.
			if _, _, err := ix.resolveReorg(ctx, head); err != nil {
				return err
			}
			continue
		}

		// Even when the tip does not advance, a node may swap the hash of an
		// unfinalized height (reorg at equal height). Verify the unfinalized
		// tail against the node before declaring nothing to do.
		if has {
			if divergedAt, ok, err := ix.verifyTail(ctx, tip); err != nil {
				return err
			} else if ok {
				rounds++
				if rounds > ix.cfg.MaxRounds {
					return ErrReorgNotConverging
				}
				if _, _, err := ix.resolveReorg(ctx, divergedAt-1); err != nil {
					return err
				}
				continue
			}
		}

		from := uint64(0)
		if has {
			from = head + 1
		}
		if from > tip {
			// Nothing new; still promote any blocks that became confirmed.
			ix.finalizeUpTo(tip)
			return nil
		}

		end := tip
		if end > from+batchWindow-1 {
			end = from + batchWindow - 1
		}

		results := ix.fetchRange(ctx, from, end)

		// Sequential validation + write: concurrent fetch scheduling must never
		// affect write order.
		reorged := false
		var prevHash string
		var genuineErr error
		for i := 0; i < len(results); i++ {
			h := from + uint64(i)
			res := results[i]
			if res.err != nil {
				if res.skip {
					// A sibling fetch failed first; keep scanning so we surface
					// that genuine error instead of this placeholder.
					continue
				}
				if errors.Is(res.err, rpc.ErrBlockNotFound) {
					if h == from {
						// The next block vanished: either a reorg or true missing
						// data. Probe from the last stored height (or genesis when
						// the store is empty) for a common ancestor.
						hint := h
						if headH, okHead := ix.cfg.Store.Head(); okHead && headH > 0 {
							hint = headH
						}
						rounds++
						if rounds > ix.cfg.MaxRounds {
							return ErrReorgNotConverging
						}
						ancestor, removed, rerr := ix.resolveReorg(ctx, hint)
						if rerr != nil {
							return rerr
						}
						// Nothing was discarded and we still cannot extend: this is
						// genuine missing data on the node, not a reorganization.
						if removed == 0 {
							return fmt.Errorf("%w at height %d: node reports tip %d (no reorg detected)",
								ErrMissingData, h, tip)
						}
						_ = ancestor
						reorged = true
						break
					}
					// A gap inside the batch while the node claims a tip past
					// it may also be a shortened chain: probe for an ancestor.
					if h <= tip {
						rounds++
						if rounds > ix.cfg.MaxRounds {
							return ErrReorgNotConverging
						}
						hint := h - 1
						if _, removed, rerr := ix.resolveReorg(ctx, hint); rerr != nil {
							return rerr
						} else if removed > 0 {
							reorged = true
							break
						}
					}
					return fmt.Errorf("%w at height %d inside range %d..%d",
						ErrMissingData, h, from, end)
				}
				genuineErr = fmt.Errorf("indexer: fetch block at height %d: %w", h, res.err)
				break
			}

			b := res.block
			if b.Number != h {
				ix.log("height=%d decision=reject reason=height-jump want=%d got=%d",
					h, h, b.Number)
				return fmt.Errorf("%w: requested %d node returned %d",
					ErrHeightJump, h, b.Number)
			}

			// Parent linkage: must extend the stored chain exactly.
			var expected string
			if h == 0 {
				expected = ""
			} else if h == from {
				if parent := ix.cfg.Store.GetBlock(h - 1); parent != nil {
					expected = parent.Hash
				}
			} else {
				expected = prevHash
			}
			if b.ParentHash != expected {
				ix.log("height=%d hash=%s decision=reorg-signal reason=parent-mismatch got-parent=%s expected-parent=%s",
					h, b.Hash, b.ParentHash, expected)
				rounds++
				if rounds > ix.cfg.MaxRounds {
					return ErrReorgNotConverging
				}
				if _, _, err := ix.resolveReorg(ctx, h-1); err != nil {
					return err
				}
				reorged = true
				break
			}

			dup, perr := ix.cfg.Store.PutBlock(b)
			if perr != nil {
				if errors.Is(perr, store.ErrGap) {
					return fmt.Errorf("%w: %v", ErrHeightJump, perr)
				}
				return fmt.Errorf("indexer: store block at height %d: %w", h, perr)
			}
			if dup {
				ix.log("height=%d hash=%s decision=duplicate reason=same-hash-already-indexed",
					h, b.Hash)
			} else {
				ix.log("height=%d hash=%s decision=index parent=%s events=%d",
					h, b.Hash, b.ParentHash, len(b.Events))
			}
			prevHash = b.Hash
		}

		if genuineErr != nil {
			return genuineErr
		}
		if reorged {
			continue
		}

		ix.finalizeUpTo(tip)
		if end >= tip {
			return nil
		}
	}
}

// fetchRange fetches [from,to] with at most cfg.Concurrency in-flight calls.
// Results are returned in height order regardless of completion order.
func (ix *Indexer) fetchRange(ctx context.Context, from, to uint64) []fetchResult {
	n := int(to-from) + 1
	results := make([]fetchResult, n)
	sem := make(chan struct{}, ix.cfg.Concurrency)
	stop := make(chan struct{})
	var stopOnce sync.Once
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		h := from + uint64(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-stop:
				results[i] = fetchResult{err: errSkipped, skip: true}
				return
			case <-ctx.Done():
				results[i] = fetchResult{err: ctx.Err()}
				return
			}
			defer func() { <-sem }()

			// If another fetch already failed, don't pile on more node calls.
			select {
			case <-stop:
				results[i] = fetchResult{err: errSkipped, skip: true}
				return
			default:
			}
			b, err := ix.cfg.Client.BlockByNumber(ctx, h)
			if err != nil {
				results[i] = fetchResult{err: err}
				// Missing data is a normal reorg/missing-data signal; it does
				// not shut down sibling fetches.
				if !errors.Is(err, rpc.ErrBlockNotFound) {
					stopOnce.Do(func() { close(stop) })
				}
				return
			}
			results[i] = fetchResult{block: b}
		}()
	}
	wg.Wait()
	return results
}

// resolveReorg walks down from hint comparing stored hashes with node hashes
// until a common ancestor is found, then rolls the store back to it.
// Returns the ancestor height and number of blocks removed.
func (ix *Indexer) resolveReorg(ctx context.Context, hint uint64) (uint64, int, error) {
	ancestor, found := ix.findCommonAncestor(ctx, hint)

	// Never silently revert finalized data: a reorg past the confirmation
	// depth is an explicit, distinguishable failure. With zero required
	// confirmations there is no finality promise to protect — blocks are
	// presented immediately and must be corrected when the node swaps them.
	protected := ix.cfg.Rule.Confirmations > 0
	if protected {
		if fh, ok := ix.cfg.Store.FinalizedHead(); ok && (!found || fh > ancestor) {
			ix.log("reorg decision=reject reason=would-revert-finalized finalized-head=%d ancestor=%d",
				fh, ancestor)
			return 0, 0, fmt.Errorf("%w: finalized head %d, common ancestor %d",
				ErrFinalityReorg, fh, ancestor)
		}
	}

	var removed int
	if !found {
		removed = ix.cfg.Store.Reset()
		ix.log("reorg decision=reset reason=no-common-ancestor removed=%d", removed)
		return 0, removed, nil
	}

	var err error
	if protected {
		removed, err = ix.cfg.Store.RollbackTo(ancestor)
	} else {
		removed, err = ix.cfg.Store.ForceRollbackTo(ancestor)
	}
	if err != nil {
		return 0, removed, fmt.Errorf("indexer: rollback to %d: %w", ancestor, err)
	}
	if b := ix.cfg.Store.GetBlock(ancestor); b != nil {
		ix.log("reorg decision=rollback common-ancestor=%d hash=%s removed=%d",
			ancestor, b.Hash, removed)
	} else {
		ix.log("reorg decision=rollback common-ancestor=%d removed=%d", ancestor, removed)
	}
	return ancestor, removed, nil
}

// findCommonAncestor compares stored vs node blocks from hint down to genesis.
func (ix *Indexer) findCommonAncestor(ctx context.Context, hint uint64) (uint64, bool) {
	for h := int64(hint); h >= 0; h-- {
		height := uint64(h)
		stored := ix.cfg.Store.GetBlock(height)
		if stored == nil {
			continue
		}
		nb, err := ix.cfg.Client.BlockByNumber(ctx, height)
		if err != nil {
			if errors.Is(err, rpc.ErrBlockNotFound) {
				ix.log("height=%d decision=reorg-probe result=absent-on-node", height)
				continue
			}
			// Non-retryable exhaustion/other failures abort: never guess.
			ix.log("height=%d decision=reorg-probe result=error err=%v", height, err)
			return 0, false
		}
		if nb.Hash == stored.Hash {
			ix.log("height=%d hash=%s decision=reorg-probe result=common-ancestor",
				height, nb.Hash)
			return height, true
		}
		ix.log("height=%d hash=%s vs-stored=%s decision=reorg-probe result=diverged",
			height, nb.Hash, stored.Hash)
	}
	// When the store is empty (e.g. the very first fetch of genesis returned
	// not-found due to a swap), any node genesis is acceptable as the implicit
	// starting point. If the store already contains a *different* genesis,
	// there is no common ancestor and the caller must reset instead.
	if _, ok := ix.cfg.Store.Head(); !ok {
		nb, err := ix.cfg.Client.BlockByNumber(ctx, 0)
		if err == nil && nb != nil && nb.Number == 0 {
			ix.log("height=0 hash=%s decision=reorg-probe result=genesis-ancestor", nb.Hash)
			return 0, true
		}
	}
	return 0, false
}

// verifyTail checks stored heights up to tip against the node and returns the
// first height whose hash differs (a reorg at equal height). With
// confirmations > 0 only the unfinalized tail is re-verified: finalized
// heights are protected and any divergence there is reported as
// ErrFinalityReorg via resolveReorg. With zero confirmations every block is
// immediately presentable, so every stored height must be re-verified — a
// same-height hash swap must be corrected no matter what was presented before.
func (ix *Indexer) verifyTail(ctx context.Context, tip uint64) (uint64, bool, error) {
	var candidates []store.BlockRecord
	if ix.cfg.Rule.Confirmations == 0 {
		candidates = ix.cfg.Store.AllBlocks()
	} else {
		candidates = ix.cfg.Store.UnfinalizedBlocks()
	}
	for _, b := range candidates {
		if b.Number > tip {
			// A stored block above the node tip is handled by the tip-rollback
			// branch in Run, not here.
			continue
		}
		nb, err := ix.cfg.Client.BlockByNumber(ctx, b.Number)
		if err != nil {
			if errors.Is(err, rpc.ErrBlockNotFound) {
				return b.Number, true, nil
			}
			return 0, false, fmt.Errorf("indexer: verify height %d: %w", b.Number, err)
		}
		if nb.Hash != b.Hash {
			ix.log("height=%d stored-hash=%s node-hash=%s decision=tail-mismatch reason=same-height-reorg",
				b.Number, b.Hash, nb.Hash)
			return b.Number, true, nil
		}
	}
	return 0, false, nil
}

// finalizeUpTo promotes blocks with enough confirmations, in height order.
func (ix *Indexer) finalizeUpTo(tip uint64) {
	for _, b := range ix.cfg.Store.UnfinalizedBlocks() {
		if !ix.cfg.Rule.IsConfirmed(b.Number, tip) {
			continue
		}
		if err := ix.cfg.Store.Finalize(b.Number); err != nil {
			// The block is present (we just listed it); surface nothing here,
			// Run callers see FinalBlocks.
			continue
		}
		ix.log("height=%d hash=%s decision=finalize reason=tip-%d >= confirmations-%d",
			b.Number, b.Hash, tip-b.Number, ix.cfg.Rule.Confirmations)
	}
}
