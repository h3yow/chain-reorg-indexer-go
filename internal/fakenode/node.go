// Package fakenode provides a programmable in-process fake chain node
// implementing rpc.Client, used to make indexer behavior deterministic.
package fakenode

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// Call records one node interaction for test assertions/logging.
type Call struct {
	Method string
	Number uint64
	At     time.Time
	Class  rpc.ErrorClass // class of an injected failure, or 0 (ClassUnknown) on success
	OK     bool
}

type failureRule struct {
	method string // "tip" or "blk"
	number int64  // -1 matches every number for that method
	class  rpc.ErrorClass
	left   int // remaining occurrences; -1 = forever
}

// Node is a programmable fake chain node safe for concurrent use.
type Node struct {
	mu sync.Mutex

	blocks map[uint64]*types.Block
	tip    uint64
	hasTip bool

	rules        []failureRule
	delay        time.Duration
	peakInFlight int
	inFlight     int
	calls        []Call
}

// New builds an empty fake node.
func New() *Node {
	return &Node{blocks: make(map[uint64]*types.Block)}
}

// SetChain installs blocks (keyed by height) and sets the tip to the max height.
func (n *Node) SetChain(blocks ...*types.Block) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocks = make(map[uint64]*types.Block, len(blocks))
	var max uint64
	for _, b := range blocks {
		cp := *b
		n.blocks[b.Number] = &cp
		if !n.hasTip || b.Number > max {
			max = b.Number
		}
	}
	n.tip = max
	n.hasTip = len(blocks) > 0
}

// SetTip overrides the reported tip without changing block data (useful to
// simulate a node that has not caught up yet).
func (n *Node) SetTip(tip uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tip = tip
	n.hasTip = true
}

// Tip returns the currently configured tip.
func (n *Node) Tip() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.tip
}

// ApplyReorg replaces blocks at heights >= forkFrom with the fork blocks and
// advances the tip to the fork tip. This is how a chain reorganization is
// simulated: the node starts serving different hashes past forkFrom.
func (n *Node) ApplyReorg(forkFrom uint64, fork ...*types.Block) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for h := forkFrom; ; h++ {
		if _, ok := n.blocks[h]; !ok {
			break
		}
		delete(n.blocks, h)
	}
	var max uint64
	for _, b := range fork {
		cp := *b
		n.blocks[b.Number] = &cp
		if b.Number > max {
			max = b.Number
		}
	}
	n.tip = max
	n.hasTip = len(fork) > 0
}

// FailNext injects the next `times` calls matching method/number with an error
// of the given class. method is "tip" or "blk"; number == -1 matches all
// heights for that method.
func (n *Node) FailNext(method string, number int64, class rpc.ErrorClass, times int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rules = append(n.rules, failureRule{method: method, number: number, class: class, left: times})
}

// FailAlways injects a permanent failure for every matching call.
func (n *Node) FailAlways(method string, number int64, class rpc.ErrorClass) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rules = append(n.rules, failureRule{method: method, number: number, class: class, left: -1})
}

// SetDelay makes every call wait d before responding.
func (n *Node) SetDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delay = d
}

// Calls returns a copy of the recorded call log.
func (n *Node) Calls() []Call {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]Call, len(n.calls))
	copy(out, n.calls)
	return out
}

// PeakInFlight returns the largest number of concurrent in-flight calls seen,
// used to verify the indexer's concurrency cap.
func (n *Node) PeakInFlight() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.peakInFlight
}

// ResetCalls clears the call log.
func (n *Node) ResetCalls() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = nil
}

// BlockByNumber implements rpc.Client.
func (n *Node) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	class, hasFailure := n.enter("blk", int64(number))
	defer n.leave()
	if err := n.wait(ctx); err != nil {
		return nil, err
	}
	if hasFailure {
		return nil, n.failError(class, fmt.Sprintf("block %d", number))
	}

	n.mu.Lock()
	b, ok := n.blocks[number]
	n.mu.Unlock()
	if !ok {
		return nil, &rpc.BlockNotFoundError{Height: number}
	}
	cp := *b
	return &cp, nil
}

// ChainTip implements rpc.Client.
func (n *Node) ChainTip(ctx context.Context) (uint64, error) {
	class, hasFailure := n.enter("tip", -1)
	defer n.leave()
	if err := n.wait(ctx); err != nil {
		return 0, err
	}
	if hasFailure {
		return 0, n.failError(class, "chain tip")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.hasTip {
		return 0, rpc.NewError(rpc.ClassPermanent, "chain tip unavailable: empty node")
	}
	return n.tip, nil
}

func (n *Node) failError(class rpc.ErrorClass, what string) error {
	switch class {
	case rpc.ClassTimeout:
		return rpc.NewError(class, "simulated timeout fetching %s", what)
	case rpc.ClassRateLimited:
		return rpc.NewError(class, "simulated 429 rate limit fetching %s", what)
	case rpc.ClassPermanent:
		return rpc.NewError(class, "simulated permanent failure fetching %s", what)
	default:
		return rpc.NewError(rpc.ClassTemporary, "simulated temporary failure fetching %s", what)
	}
}

func (n *Node) wait(ctx context.Context) error {
	n.mu.Lock()
	d := n.delay
	n.mu.Unlock()
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// enter records the call start, applies any scripted failure and tracks the
// in-flight concurrency.
func (n *Node) enter(method string, number int64) (rpc.ErrorClass, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.inFlight++
	if n.inFlight > n.peakInFlight {
		n.peakInFlight = n.inFlight
	}

	class := rpc.ClassUnknown
	failed := false
	for i := range n.rules {
		r := &n.rules[i]
		if r.method != method || (r.number != -1 && r.number != number) {
			continue
		}
		if r.left == 0 {
			continue
		}
		class = r.class
		failed = true
		if r.left > 0 {
			r.left--
		}
		break
	}
	recNumber := uint64(0)
	if method == "blk" {
		recNumber = uint64(number)
	}
	n.calls = append(n.calls, Call{
		Method: method,
		Number: recNumber,
		At:     time.Now(),
		Class:  class,
		OK:     !failed,
	})
	return class, failed
}

func (n *Node) leave() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.inFlight--
}

var _ rpc.Client = (*Node)(nil)
