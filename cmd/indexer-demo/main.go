// Command indexer-demo runs the indexer against the in-process fake node and
// prints every decision (height, hash, reason). It also injects rate-limited
// responses and a chain reorganization so the behavior is observable locally.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/indexer"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

func main() {
	ctx := context.Background()

	// 1. Programmable fake node with an initial 10-block chain (tip 9).
	node := fakenode.New()
	node.SetChain(fakenode.BuildLinear("A", 0, 10)...)
	node.FailNext("blk", -1, rpc.ClassRateLimited, 2) // first 2 block calls are 429

	client := rpc.NewRetryClient(node, rpc.RetryPolicy{
		MaxAttempts: 4,
		BaseBackoff: 20 * time.Millisecond,
		MaxBackoff:  200 * time.Millisecond,
	})
	st := store.New()
	ix := indexer.New(indexer.Config{
		Client:      client,
		Store:       st,
		Rule:        types.ConfirmationRule{Confirmations: 3},
		Concurrency: 3,
	})
	logger := log.New(log.Writer(), "[indexer] ", log.Ltime|log.Lmicroseconds)
	ix.SetLogger(func(format string, args ...any) {
		logger.Printf(format, args...)
	})

	fmt.Println("== phase 1: initial index (with 2 injected rate-limit errors) ==")
	if err := ix.Run(ctx); err != nil {
		log.Fatalf("initial index failed: %v", err)
	}
	fmt.Printf("indexed blocks=%d finalized=%d events=%d peak-concurrency=%d\n\n",
		len(st.AllBlocks()), len(st.FinalBlocks()), st.EventCount(), node.PeakInFlight())

	fmt.Println("== phase 2: repeat run (must be idempotent, no duplicate records) ==")
	before := st.EventCount()
	if err := ix.Run(ctx); err != nil {
		log.Fatalf("repeat index failed: %v", err)
	}
	fmt.Printf("events before=%d after=%d (must be equal)\n\n", before, st.EventCount())

	fmt.Println("== phase 3: chain reorg at height 7, tip advances to 11 ==")
	fork := fakenode.BuildFork("A", 7, 5) // 7..11
	node.ApplyReorg(7, fork...)
	if err := ix.Run(ctx); err != nil {
		log.Fatalf("reorg index failed: %v", err)
	}
	fmt.Println("final chain hashes after reorg:")
	for _, b := range st.AllBlocks() {
		fin := "unconfirmed"
		if b.Finalized {
			fin = "finalized"
		}
		fmt.Printf("  height=%2d hash=%s %s\n", b.Number, b.Hash, fin)
	}
	fmt.Printf("blocks=%d events=%d\n", len(st.AllBlocks()), st.EventCount())
}
