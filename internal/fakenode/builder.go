package fakenode

import (
	"fmt"
	"hash/fnv"

	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// deterministicHash returns a stable short hash for a chain identity/height.
func deterministicHash(chainID string, height uint64) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(chainID))
	_, _ = h.Write([]byte{byte(height), byte(height >> 8), byte(height >> 16), byte(height >> 24)})
	return fmt.Sprintf("0x%016x", h.Sum64())
}

// BuildLinear builds n blocks on one chain identity starting at startHeight.
// Genesis (startHeight) gets a zero parent; each block carries one event so
// event indexing and dedup are observable.
func BuildLinear(chainID string, startHeight, n uint64) []*types.Block {
	blocks := make([]*types.Block, 0, n)
	for i := uint64(0); i < n; i++ {
		height := startHeight + i
		hash := deterministicHash(chainID, height)
		parent := ""
		if height > 0 {
			parent = deterministicHash(chainID, height-1)
		}
		b := &types.Block{
			Number:     height,
			Hash:       hash,
			ParentHash: parent,
			Events: []types.Event{{
				Index:   0,
				Kind:    "Tick",
				Payload: fmt.Sprintf("%s@%d", chainID, height),
			}},
		}
		b.Events[0].SourceHash = hash
		blocks = append(blocks, b)
	}
	return blocks
}

// BuildFork builds a forking chain that diverges from forkHeight on chainID.
// The returned blocks start AT forkHeight (with parent equal to the linear
// chain's block at forkHeight-1) and form a new branch of n blocks using a
// distinct identity suffix so hashes never collide with the old branch.
func BuildFork(chainID string, forkHeight, n uint64) []*types.Block {
	forkID := chainID + "/fork"
	blocks := make([]*types.Block, 0, n)
	for i := uint64(0); i < n; i++ {
		height := forkHeight + i
		hash := deterministicHash(forkID, height)
		parent := ""
		switch {
		case height == 0:
			parent = ""
		case height == forkHeight:
			parent = deterministicHash(chainID, height-1) // attach to common ancestor
		default:
			parent = deterministicHash(forkID, height-1)
		}
		b := &types.Block{
			Number:     height,
			Hash:       hash,
			ParentHash: parent,
			Events: []types.Event{{
				Index:      0,
				Kind:       "Tick",
				Payload:    fmt.Sprintf("%s@%d", forkID, height),
				SourceHash: hash,
			}},
		}
		blocks = append(blocks, b)
	}
	return blocks
}
