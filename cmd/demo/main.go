// Command demo runs the incremental indexer against an in-process fake node:
// it indexes a chain, simulates a reorg plus rate limiting, and prints the
// final index state. Everything is local and fully reproducible.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/indexer"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ltime)

	// 1) Programmable local node with an initial chain a-0..a-7.
	chain := fakenode.BuildChain(0, 8, "", "a")
	node := fakenode.New(chain)
	defer node.Close()

	client := rpc.NewHTTPClient(rpc.Config{
		Endpoint: node.URL(), TimeoutMillis: 1500,
		MaxRetries: 3, BackoffMillis: 5,
	})
	defer client.Close()

	st := store.NewMemory()
	idx := indexer.New(client, st, indexer.Config{
		Confirmations: 2, MaxConcurrency: 4, MaxReorgDepth: 10,
	}, logger)

	// 2) First indexing pass.
	fmt.Println("== initial indexing ==")
	if err := idx.Run(); err != nil {
		log.Fatalf("initial indexing failed: %v", err)
	}
	fmt.Printf("confirmed blocks=%d events=%d\n\n", st.BlockCount(), st.EventCount())

	// 3) Two rate-limited responses then a reorg: replace heights 6..7 with
	//    a longer b-fork 6..8.
	node.SetFailure("eth_getBlockByNumber", fakenode.FailRateLimit, 2)
	fork := fakenode.BuildChain(6, 3, node.CanonicalHash(5), "b")
	node.SetReorg(5, fork)

	fmt.Println("== after rate-limit + reorg ==")
	if err := idx.Run(); err != nil {
		log.Fatalf("post-reorg indexing failed: %v", err)
	}
	fmt.Printf("confirmed blocks=%d events=%d\n", st.BlockCount(), st.EventCount())
	for _, h := range st.Heights() {
		b, _ := st.BlockAt(h)
		fmt.Printf("height=%d hash=%s\n", h, b.Hash)
	}
}
