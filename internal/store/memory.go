package store

import (
	"fmt"
	"sort"
	"sync"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

// MemoryStore is a thread-safe in-memory index.
// Confirmed blocks must be appended in strictly increasing height order.
type MemoryStore struct {
	mu     sync.Mutex
	blocks map[uint64]*model.Block
	events int
}

// NewMemory constructs an empty MemoryStore.
func NewMemory() *MemoryStore {
	return &MemoryStore{blocks: make(map[uint64]*model.Block)}
}

// AppendConfirmed records a confirmed block and its events.
//
// Rules:
//   - height must be lastConfirmed+1 (except the first append which may be
//     any height >= 1, to support a configured start height);
//   - appending the exact same (height, hash) again is a no-op, so a
//     repeated pull of identical data never creates duplicate rows;
//   - appending a different hash at an already stored height is rejected:
//     a reorg must be handled via RollbackTo first.
func (s *MemoryStore) AppendConfirmed(b *model.Block) error {
	if b == nil {
		return fmt.Errorf("%w: nil block", ErrGap)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.blocks[b.Number]; ok {
		if existing.Hash == b.Hash {
			// Idempotent replay of identical data.
			return nil
		}
		return fmt.Errorf("%w: height %d already confirmed with hash %s, got %s",
			ErrAlreadyConfirmed, b.Number, existing.Hash, b.Hash)
	}

	if len(s.blocks) > 0 {
		last := s.lastHeightLocked()
		if b.Number != last+1 {
			return fmt.Errorf("%w: want height %d, got %d", ErrGap, last+1, b.Number)
		}
		if prev, ok := s.blocks[b.Number-1]; ok && b.ParentHash != prev.Hash {
			return fmt.Errorf("%w: height %d parent %s != stored %s",
				ErrParentMismatch, b.Number, b.ParentHash, prev.Hash)
		}
	}

	cp := *b
	cp.Events = append([]model.Event(nil), b.Events...)
	s.blocks[cp.Number] = &cp
	s.events += len(cp.Events)
	return nil
}

// RollbackTo removes every confirmed record above height.
func (s *MemoryStore) RollbackTo(height uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for h, b := range s.blocks {
		if h > height {
			s.events -= len(b.Events)
			delete(s.blocks, h)
		}
	}
	return nil
}

// LastConfirmed returns the highest confirmed height and its block hash.
func (s *MemoryStore) LastConfirmed() (uint64, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.blocks) == 0 {
		return 0, "", false
	}
	h := s.lastHeightLocked()
	return h, s.blocks[h].Hash, true
}

// BlockAt returns the stored block at height.
func (s *MemoryStore) BlockAt(height uint64) (*model.Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.blocks[height]
	if !ok {
		return nil, fmt.Errorf("%w: height %d", ErrNotFound, height)
	}
	cp := *b
	return &cp, nil
}

// EventCount returns the number of indexed event rows.
func (s *MemoryStore) EventCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events
}

// BlockCount returns the number of indexed block rows.
func (s *MemoryStore) BlockCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.blocks)
}

// Heights returns all stored heights in ascending order (test helper).
func (s *MemoryStore) Heights() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint64, 0, len(s.blocks))
	for h := range s.blocks {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (s *MemoryStore) lastHeightLocked() uint64 {
	var h uint64
	for k := range s.blocks {
		if k > h {
			h = k
		}
	}
	return h
}
