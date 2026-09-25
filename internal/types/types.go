package types

import "fmt"

// Block represents a block returned by a chain node.
type Block struct {
	Number     uint64
	Hash       string
	ParentHash string
	Events     []Event
}

// Event represents a log/event contained in a block.
type Event struct {
	Index      int    // position within the block
	SourceHash string // hash of the containing block
	Kind       string
	Payload    string
}

// Key returns the unique identity of an event (block hash + event index).
func (e Event) Key() string {
	return fmt.Sprintf("%s#%d", e.SourceHash, e.Index)
}

// ConfirmationRule describes when a block may be treated as final.
type ConfirmationRule struct {
	// Confirmations is the number of follow-up blocks required.
	// A block at height h is confirmed when the node tip >= h+Confirmations.
	Confirmations uint64
}

// IsConfirmed reports whether blockHeight is confirmed given the node tip.
func (r ConfirmationRule) IsConfirmed(blockHeight, tip uint64) bool {
	if tip < blockHeight {
		return false
	}
	return tip-blockHeight >= r.Confirmations
}
