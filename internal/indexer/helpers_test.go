package indexer

import (
	"fmt"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/store"
)

// testLogger prints to the test log so every decision (height, hash, reason)
// is visible in `go test -v` output.
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("[index] "+format, args...)
}

type harness struct {
	t       *testing.T
	node    *fakenode.Node
	client  rpc.Client
	st      *store.MemoryStore
	idx     *Indexer
	closers []func()
}

func newHarness(t *testing.T, chain []*model.Block, cfg Config) *harness {
	t.Helper()
	n := fakenode.New(chain)
	c := rpc.NewHTTPClient(rpc.Config{
		Endpoint: n.URL(), TimeoutMillis: 1500,
		MaxRetries: 3, BackoffMillis: 2,
	})
	st := store.NewMemory()
	idx := New(c, st, cfg, testLogger{t})
	h := &harness{t: t, node: n, client: c, st: st, idx: idx}
	h.closers = append(h.closers, c.Close, n.Close)
	t.Cleanup(func() {
		for i := len(h.closers) - 1; i >= 0; i-- {
			h.closers[i]()
		}
	})
	return h
}

// runUntilIdle keeps stepping until no progress or an error occurs.
func (h *harness) runUntilIdle() error {
	for i := 0; i < 1000; i++ {
		moved, err := h.idx.Step()
		if err != nil {
			return err
		}
		if !moved {
			return nil
		}
	}
	return fmt.Errorf("indexer did not reach idle after 1000 steps")
}

func (h *harness) mustFinalHeights(want []uint64) {
	h.t.Helper()
	got := h.st.Heights()
	if len(got) != len(want) {
		h.t.Fatalf("final heights = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			h.t.Fatalf("final heights = %v, want %v", got, want)
		}
	}
}

func (h *harness) stHashAt(height uint64) string {
	b, err := h.st.BlockAt(height)
	if err != nil {
		return ""
	}
	return b.Hash
}
