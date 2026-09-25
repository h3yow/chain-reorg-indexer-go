package indexer

import (
	"errors"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
)

// Indexing errors, distinguishable via errors.Is.
var (
	// ErrHeightGap: requested/pulled heights skip a missing block,
	// or the node returned no block where one was required.
	ErrHeightGap = errors.New("indexer: height gap or missing block")
	// ErrParentHashMismatch: a block's parentHash does not connect to
	// the previous block, and no reorg could be resolved within depth.
	ErrParentHashMismatch = errors.New("indexer: parent hash discontinuity")
	// ErrReorgTooDeep: rollback point lies beyond MaxReorgDepth.
	ErrReorgTooDeep = errors.New("indexer: reorg deeper than configured max depth")
	// ErrRetryExhausted: node kept failing with a retryable error beyond
	// the client's retry budget. Wraps the underlying rpc sentinel.
	ErrRetryExhausted = errors.New("indexer: retry budget exhausted")
	// ErrChainBehind: node head height is below the local confirmed tip.
	ErrChainBehind = errors.New("indexer: node reports head behind local index")
)

// Logger is the minimal logging surface.
type Logger interface {
	Printf(format string, args ...any)
}

// Config controls confirmation and fetch behaviour.
type Config struct {
	// Confirmations is the number of blocks that must follow a block
	// before it is presented as final. 0 = the tip itself is final.
	Confirmations int
	// MaxConcurrency bounds concurrent block fetches per cycle.
	MaxConcurrency int
	// MaxReorgDepth bounds how far back a reorg rollback may reach.
	MaxReorgDepth int
	// StartHeight is the first height to index (when store is empty).
	StartHeight uint64
}

// Indexer incrementally indexes blocks/events with a confirmation policy,
// dedup, reorg rollback and order-preserving concurrent fetching.
type Indexer struct {
	client rpc.Client
	store  store.Store
	cfg    Config
	log    Logger

	// tip caches the most recently observed node tip for one cycle.
	tip *rpcHead

	// pending caches fetched-but-unconfirmed blocks by height between
	// cycles, so advancing confirmations does not re-pull identical data.
	// Cleared on rollback and on hash change at a cached height.
	pending map[uint64]*model.Block
}

type rpcHead struct {
	number uint64
	hash   string
}

// New builds an Indexer.
func New(client rpc.Client, st store.Store, cfg Config, log Logger) *Indexer {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	if cfg.Confirmations < 0 {
		cfg.Confirmations = 0
	}
	if cfg.MaxReorgDepth < 0 {
		cfg.MaxReorgDepth = 0
	}
	return &Indexer{
		client:  client,
		store:   st,
		cfg:     cfg,
		log:     log,
		pending: make(map[uint64]*model.Block),
	}
}

// Run advances repeatedly until the confirmable tip is consumed.
func (x *Indexer) Run() error {
	for {
		moved, err := x.Step()
		if err != nil {
			return err
		}
		if !moved {
			return nil
		}
	}
}
