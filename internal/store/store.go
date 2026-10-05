// Package store holds indexed blocks/events and supports reorg rollback.
package store

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// BlockRecord is a block stored with its finalized flag.
type BlockRecord struct {
	types.Block
	Finalized bool
}

var (
	// ErrGap is returned when a block is inserted beyond head+1, i.e. a height
	// jump / missing height.
	ErrGap = errors.New("store: height gap, missing block")
	// ErrConflict is returned when a different block already occupies a height.
	ErrConflict = errors.New("store: conflicting block at the same height")
	// ErrFinalizedRollback is returned when rollback would remove a finalized
	// block (a reorg deeper than the configured finality depth).
	ErrFinalizedRollback = errors.New("store: cannot roll back finalized block")
)

// Store is the local index store.
//
// Indexed blocks form a strictly contiguous chain up to Head(): a PutBlock at
// head+2 is rejected with ErrGap. Rollback truncates the unfinalized tail so a
// reorged-out branch can never leave residue.
type Store struct {
	mu     sync.Mutex
	blocks map[uint64]*BlockRecord
	events map[string]types.Event
	head   uint64
	has    bool
}

// New creates an empty store.
func New() *Store {
	return &Store{
		blocks: make(map[uint64]*BlockRecord),
		events: make(map[string]types.Event),
	}
}

// PutBlock stores a block and its events.
//
// Re-pulling the exact same block at the same height is a no-op and never
// produces duplicate records (Dup == true). A different block at an occupied
// height returns ErrConflict; a height jump returns ErrGap.
func (s *Store) PutBlock(b *types.Block) (dup bool, err error) {
	if b == nil {
		return false, errors.New("store: nil block")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.blocks[b.Number]; ok {
		if existing.Hash == b.Hash {
			return true, nil // idempotent re-pull, no duplicate records
		}
		return false, fmt.Errorf("%w %d: stored %s, got %s",
			ErrConflict, b.Number, existing.Hash, b.Hash)
	}

	want := uint64(0)
	if s.has {
		want = s.head + 1
	}
	if b.Number != want {
		return false, fmt.Errorf("%w: want height %d, got %d", ErrGap, want, b.Number)
	}

	rec := &BlockRecord{Block: *b}
	s.blocks[b.Number] = rec
	for _, ev := range b.Events {
		if ev.SourceHash == "" {
			ev.SourceHash = b.Hash
		}
		if _, exists := s.events[ev.Key()]; !exists {
			s.events[ev.Key()] = ev
		}
	}
	if !s.has || b.Number > s.head {
		s.head = b.Number
		s.has = true
	}
	return false, nil
}

// Finalize marks the block at height as finalized/confirmed.
func (s *Store) Finalize(height uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocks[height]
	if !ok {
		return fmt.Errorf("store: cannot finalize missing height %d", height)
	}
	b.Finalized = true
	return nil
}

// GetBlock returns the stored block at height, or nil.
func (s *Store) GetBlock(height uint64) *BlockRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok := s.blocks[height]; ok {
		cp := *b
		return &cp
	}
	return nil
}

// Head returns the highest stored height, or false if empty.
func (s *Store) Head() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head, s.has
}

// FinalizedHead returns the highest finalized height, or false if none.
func (s *Store) FinalizedHead() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has {
		return 0, false
	}
	for h := s.head; ; h-- {
		if s.blocks[h].Finalized {
			return h, true
		}
		if h == 0 {
			return 0, false
		}
	}
}

// RollbackTo removes every stored block with height > keepHeight, along with
// that branch's events. Removing a finalized block fails explicitly.
func (s *Store) RollbackTo(keepHeight uint64) (int, error) {
	return s.rollbackTo(keepHeight, false)
}

// RollbackToForced is RollbackTo without the finalized-block guard. It exists
// for configurations with zero required confirmations, where blocks are
// presented as final immediately and a later reorg must still be able to
// correct them.
func (s *Store) RollbackToForced(keepHeight uint64) (int, error) {
	return s.rollbackTo(keepHeight, true)
}

func (s *Store) rollbackTo(keepHeight uint64, allowFinalized bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	if s.has && keepHeight < s.head {
		for h := keepHeight + 1; h <= s.head; h++ {
			b, ok := s.blocks[h]
			if !ok {
				continue
			}
			if b.Finalized && !allowFinalized {
				return removed, fmt.Errorf("%w at height %d", ErrFinalizedRollback, h)
			}
			for _, ev := range b.Events {
				delete(s.events, ev.Key())
			}
			delete(s.blocks, h)
			removed++
		}
	}
	if keepHeight == 0 {
		if _, ok := s.blocks[0]; ok {
			s.head = 0
		} else {
			s.has = false
		}
	}
	if s.has {
		if _, ok := s.blocks[keepHeight]; ok {
			s.head = keepHeight
		} else {
			s.has = false
		}
	}
	return removed, nil
}

// AllBlocks returns all stored blocks in ascending height order.
func (s *Store) AllBlocks() []BlockRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rangeLocked(0, s.head, s.has)
}

// FinalBlocks returns finalized blocks in ascending height order.
func (s *Store) FinalBlocks() []BlockRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has {
		return nil
	}
	var out []BlockRecord
	for h := uint64(0); h <= s.head; h++ {
		if b, ok := s.blocks[h]; ok && b.Finalized {
			out = append(out, *b)
		}
	}
	return out
}

// UnfinalizedBlocks returns non-finalized blocks in ascending height order.
func (s *Store) UnfinalizedBlocks() []BlockRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has {
		return nil
	}
	var out []BlockRecord
	for h := uint64(0); h <= s.head; h++ {
		if b, ok := s.blocks[h]; ok && !b.Finalized {
			out = append(out, *b)
		}
	}
	return out
}

func (s *Store) rangeLocked(from, to uint64, ok bool) []BlockRecord {
	if !ok {
		heights := make([]uint64, 0, len(s.blocks))
		for h := range s.blocks {
			heights = append(heights, h)
		}
		sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
		out := make([]BlockRecord, 0, len(heights))
		for _, h := range heights {
			out = append(out, *s.blocks[h])
		}
		return out
	}
	out := make([]BlockRecord, 0, to-from+1)
	for h := from; h <= to; h++ {
		if b, exists := s.blocks[h]; exists {
			out = append(out, *b)
		}
	}
	return out
}

// EventCount returns the number of stored unique events.
func (s *Store) EventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// HasEvent reports whether an event key is present in the index.
func (s *Store) HasEvent(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.events[key]
	return ok
}

// Reset empties the store entirely. It is used when a reorg has no common
// ancestor with the previously indexed chain.
func (s *Store) Reset() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := len(s.blocks)
	s.blocks = make(map[uint64]*BlockRecord)
	s.events = make(map[string]types.Event)
	s.head = 0
	s.has = false
	return removed
}
