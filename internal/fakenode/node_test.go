package fakenode_test

import (
	"context"
	"errors"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
)

func TestFakeNode_ScriptedFailuresAndLog(t *testing.T) {
	node := fakenode.New()
	node.SetChain(fakenode.BuildLinear("A", 0, 4)...)

	node.FailNext("blk", 2, rpc.ClassRateLimited, 1)
	node.FailNext("tip", -1, rpc.ClassTimeout, 2)

	ctx := context.Background()

	// Two tip calls fail with the timeout class, third succeeds.
	for i := 0; i < 2; i++ {
		if _, err := node.ChainTip(ctx); rpc.ClassOf(err) != rpc.ClassTimeout {
			t.Fatalf("tip call %d: class=%v want timeout (%v)", i, rpc.ClassOf(err), err)
		}
	}
	if tip, err := node.ChainTip(ctx); err != nil || tip != 3 {
		t.Fatalf("tip=%d err=%v want 3 nil", tip, err)
	}

	// Block 2 fails once with rate limit, then serves normally.
	if _, err := node.BlockByNumber(ctx, 2); rpc.ClassOf(err) != rpc.ClassRateLimited {
		t.Fatalf("class=%v want rate_limited", rpc.ClassOf(err))
	}
	b, err := node.BlockByNumber(ctx, 2)
	if err != nil || b == nil || b.Number != 2 {
		t.Fatalf("retry block 2: b=%v err=%v", b, err)
	}

	// Missing height is a non-retryable not-found.
	if _, err := node.BlockByNumber(ctx, 99); !errors.Is(err, rpc.ErrBlockNotFound) {
		t.Fatalf("err=%v want ErrBlockNotFound", err)
	}

	log := node.Calls()
	var blk2OK int
	for _, c := range log {
		if c.Method == "blk" && c.Number == 2 && c.OK {
			blk2OK++
		}
	}
	if blk2OK != 1 {
		t.Fatalf("successful blk-2 calls=%d want 1", blk2OK)
	}
}

func TestFakeNode_ReorgSwapsHashes(t *testing.T) {
	old := fakenode.BuildLinear("A", 0, 6)
	node := fakenode.New()
	node.SetChain(old...)

	before, _ := node.BlockByNumber(context.Background(), 4)
	fork := fakenode.BuildFork("A", 4, 3)
	node.ApplyReorg(4, fork...)

	after, err := node.BlockByNumber(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if after.Hash == before.Hash {
		t.Fatal("fork hash must differ from old chain")
	}
	if after.ParentHash != old[3].Hash {
		t.Fatal("fork block must attach to the common ancestor")
	}
	if tip, _ := node.ChainTip(context.Background()); tip != 6 {
		t.Fatalf("tip=%d want 6", tip)
	}
}
