package store

import (
	"errors"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

func mkBlock(n uint64, hash, parent string, events int) *model.Block {
	b := &model.Block{Number: n, Hash: hash, ParentHash: parent}
	for i := 0; i < events; i++ {
		b.Events = append(b.Events, model.Event{
			Index: i, BlockNumber: n, BlockHash: hash,
			Kind: "Transfer", Payload: hash,
		})
	}
	return b
}

func TestAppendConfirmedIdempotent(t *testing.T) {
	s := NewMemory()
	b1 := mkBlock(1, "h1", "h0", 2)
	if err := s.AppendConfirmed(b1); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// Repeated pull of the exact same data must not duplicate rows.
	if err := s.AppendConfirmed(mkBlock(1, "h1", "h0", 2)); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if got := s.BlockCount(); got != 1 {
		t.Fatalf("block count = %d, want 1 (no duplicates on repeated pull)", got)
	}
	if got := s.EventCount(); got != 2 {
		t.Fatalf("event count = %d, want 2", got)
	}
}

func TestAppendConfirmedRejectsDifferentHash(t *testing.T) {
	s := NewMemory()
	_ = s.AppendConfirmed(mkBlock(1, "h1", "h0", 0))
	err := s.AppendConfirmed(mkBlock(1, "h1-fork", "h0", 0))
	if !errors.Is(err, ErrAlreadyConfirmed) {
		t.Fatalf("want ErrAlreadyConfirmed, got %v", err)
	}
}

func TestAppendConfirmedRejectsGap(t *testing.T) {
	s := NewMemory()
	_ = s.AppendConfirmed(mkBlock(1, "h1", "h0", 0))
	err := s.AppendConfirmed(mkBlock(3, "h3", "h2", 0))
	if !errors.Is(err, ErrGap) {
		t.Fatalf("want ErrGap for height jump, got %v", err)
	}
}

func TestAppendConfirmedRejectsParentMismatch(t *testing.T) {
	s := NewMemory()
	_ = s.AppendConfirmed(mkBlock(1, "h1", "h0", 0))
	err := s.AppendConfirmed(mkBlock(2, "h2", "not-h1", 0))
	if !errors.Is(err, ErrParentMismatch) {
		t.Fatalf("want ErrParentMismatch, got %v", err)
	}
}

func TestRollbackRemovesForkData(t *testing.T) {
	s := NewMemory()
	_ = s.AppendConfirmed(mkBlock(1, "h1", "h0", 1))
	_ = s.AppendConfirmed(mkBlock(2, "h2", "h1", 1))
	_ = s.AppendConfirmed(mkBlock(3, "h3", "h2", 1))
	if err := s.RollbackTo(1); err != nil {
		t.Fatal(err)
	}
	if got := s.BlockCount(); got != 1 {
		t.Fatalf("after rollback block count = %d, want 1", got)
	}
	if got := s.EventCount(); got != 1 {
		t.Fatalf("after rollback event count = %d, want 1", got)
	}
	if h, hash, ok := s.LastConfirmed(); !ok || h != 1 || hash != "h1" {
		t.Fatalf("last confirmed = (%d,%s,%v), want (1,h1,true)", h, hash, ok)
	}
	// Re-append new chain tail with no leftover state.
	if err := s.AppendConfirmed(mkBlock(2, "p2", "h1", 3)); err != nil {
		t.Fatalf("re-append after rollback: %v", err)
	}
	if got := s.EventCount(); got != 4 {
		t.Fatalf("event count = %d, want 4", got)
	}
}
