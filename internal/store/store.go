package store

import (
	"errors"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

// Index state errors.
var (
	ErrAlreadyConfirmed = errors.New("store: height already finalized")
	ErrGap              = errors.New("store: height gap or missing data")
	ErrParentMismatch   = errors.New("store: parent hash mismatch")
	ErrNotFound         = errors.New("store: no record at height")
)

// Store is the durable-ish index surface used by the indexer.
type Store interface {
	// AppendConfirmed records a block + events at height h.
	// Must be idempotent for the same (height, hash): a duplicate
	// pull of identical data must not create duplicate records.
	AppendConfirmed(b *model.Block) error

	// RollbackTo removes all confirmed records with height > height.
	RollbackTo(height uint64) error

	// LastConfirmed returns the highest confirmed height and its hash.
	LastConfirmed() (uint64, string, bool)

	// BlockAt returns the stored block at height.
	BlockAt(height uint64) (*model.Block, error)

	// EventCount returns total indexed event rows.
	EventCount() int

	// BlockCount returns total indexed block rows.
	BlockCount() int
}
