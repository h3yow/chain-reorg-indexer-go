package fakenode

import (
	"fmt"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

// BuildChain builds n blocks starting at height start with chained parent
// hashes. tag disambiguates forks (e.g. "a" vs "b"). Every block carries one
// deterministic "Transfer" event.
func BuildChain(start uint64, n int, parent string, tag string) []*model.Block {
	out := make([]*model.Block, 0, n)
	prev := parent
	for i := 0; i < n; i++ {
		h := start + uint64(i)
		hash := fmt.Sprintf("0x%s-%d", tag, h)
		b := &model.Block{Number: h, Hash: hash, ParentHash: prev}
		b.Events = append(b.Events, model.Event{
			Index: 0, BlockNumber: h, BlockHash: hash,
			Kind: "Transfer", Payload: fmt.Sprintf("evt-%s-%d", tag, h),
		})
		out = append(out, b)
		prev = hash
	}
	return out
}
