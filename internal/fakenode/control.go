package fakenode

import (
	"fmt"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

// SetReorg replaces the canonical chain tail strictly above baseHeight
// with forkBlocks. The fork must be parent-hash connected and anchored at
// the current canonical block of baseHeight.
func (n *Node) SetReorg(baseHeight uint64, forkBlocks []*model.Block) {
	n.mu.Lock()
	defer n.mu.Unlock()

	anchor, ok := n.canonical[baseHeight]
	if !ok {
		panic(fmt.Sprintf("fakenode: no canonical block at base height %d", baseHeight))
	}
	for i, b := range forkBlocks {
		wantH := baseHeight + uint64(i) + 1
		if b.Number != wantH {
			panic(fmt.Sprintf("fakenode: fork block %d has height %d, want %d", i, b.Number, wantH))
		}
		if i == 0 {
			if b.ParentHash != anchor {
				panic(fmt.Sprintf("fakenode: fork not anchored at %d: parent %s want %s",
					baseHeight, b.ParentHash, anchor))
			}
		} else if b.ParentHash != forkBlocks[i-1].Hash {
			panic(fmt.Sprintf("fakenode: fork disconnected at height %d", b.Number))
		}
	}

	for h := range n.canonical {
		if h > baseHeight {
			delete(n.canonical, h)
		}
	}
	for _, b := range forkBlocks {
		n.pool[b.Hash] = b
		n.canonical[b.Number] = b.Hash
	}
}

// SetFailure schedules mode for the next `times` calls of method.
func (n *Node) SetFailure(method string, mode FailureMode, times int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.failures[method] = &failRule{mode: mode, times: times}
}

// SetLatencyMillis configures artificial per-response latency.
func (n *Node) SetLatencyMillis(ms int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.latencyMS = ms
}

// Calls returns a copy of the recorded call sequence.
func (n *Node) Calls() []Call {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Call, len(n.calls))
	copy(out, n.calls)
	return out
}

// CanonicalHeight returns the current canonical tip height.
func (n *Node) CanonicalHeight() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	var h uint64
	for k := range n.canonical {
		if k >= h {
			h = k
		}
	}
	return h
}

// CanonicalHash returns the canonical hash at height ("" if absent).
func (n *Node) CanonicalHash(height uint64) string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.canonical[height]
}

// toWire converts a model block to the JSON wire shape.
func toWire(b *model.Block) *blockWire {
	w := &blockWire{Number: b.Number, Hash: b.Hash, ParentHash: b.ParentHash}
	for _, e := range b.Events {
		w.Events = append(w.Events, &eventWire{Index: e.Index, Kind: e.Kind, Payload: e.Payload})
	}
	return w
}

// Extend appends blocks on top of the current canonical tip.
func (n *Node) Extend(blocks []*model.Block) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var tip uint64
	tipHash := ""
	for h, hash := range n.canonical {
		if h >= tip || tipHash == "" {
			tip, tipHash = h, hash
		}
	}
	prev := tipHash
	for i, b := range blocks {
		wantH := tip + uint64(i) + 1
		if b.Number != wantH || b.ParentHash != prev {
			panic("fakenode: Extend blocks must connect to the canonical tip")
		}
		n.pool[b.Hash] = b
		n.canonical[b.Number] = b.Hash
		prev = b.Hash
	}
}
