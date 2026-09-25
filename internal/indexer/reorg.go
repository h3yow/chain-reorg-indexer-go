package indexer

import (
	"errors"
	"fmt"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
)

// findCommonAncestor walks the confirmed chain backwards from startH and
// compares each stored hash with what the node currently calls canonical.
// The first matching height is the common ancestor.
//
// startH itself is probed first (step 0): in the parent-mismatch path it is
// the confirmed anchor and may still match, in which case nothing confirmed
// is rolled back.
//
// The walk is bounded by MaxReorgDepth, i.e. the number of confirmed blocks
// that may be discarded (ancestor at startH-k discards k blocks). If no
// ancestor is found within that many steps (or it runs below the indexed
// range), ErrReorgTooDeep is returned explicitly rather than silently
// truncating history.
func (x *Indexer) findCommonAncestor(startH uint64, _ string) (uint64, string, error) {
	for step := 0; step <= x.cfg.MaxReorgDepth; step++ {
		if startH < uint64(step) {
			break // underflow guard: fell below height 0
		}
		h := startH - uint64(step)

		stored, err := x.store.BlockAt(h)
		if err != nil {
			break // below our own indexed range; ancestry cannot be proven
		}
		nb, err := x.client.GetBlockByNumber(h)
		if err != nil {
			if errors.Is(err, rpc.ErrBlockMissing) {
				continue
			}
			return 0, "", x.wrapRPC(err)
		}
		x.logf("ANCESTOR-PROBE height=%d stored=%s node=%s match=%v",
			h, short(stored.Hash), short(nb.Hash), stored.Hash == nb.Hash)
		if nb.Hash == stored.Hash {
			return h, stored.Hash, nil
		}
	}
	return 0, "", fmt.Errorf("%w: no common ancestor within %d blocks from height %d",
		ErrReorgTooDeep, x.cfg.MaxReorgDepth, startH)
}
