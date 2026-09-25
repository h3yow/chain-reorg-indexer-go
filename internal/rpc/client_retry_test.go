package rpc

import (
	"errors"
	"strings"
	"testing"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

func newNode(t *testing.T) (*fakenode.Node, *HTTPClient) {
	t.Helper()
	chain := fakenode.BuildChain(0, 3, "", "a")
	n := fakenode.New(chain)
	c := NewHTTPClient(Config{
		Endpoint:      n.URL(),
		TimeoutMillis: 1500,
		MaxRetries:    3,
		BackoffMillis: 2,
	})
	t.Cleanup(func() { c.Close(); n.Close() })
	return n, c
}

// Succeeds after two temporary failures: retries must recover transparently.
func TestRetryRecoversFromTemporaryFailures(t *testing.T) {
	n, c := newNode(t)
	n.SetFailure("eth_getBlockByNumber", fakenode.FailTemporary, 2)
	b, err := c.GetBlockByNumber(1)
	if err != nil {
		t.Fatalf("expected recovery after retries, got %v", err)
	}
	if b.Hash != "0xa-1" {
		t.Fatalf("hash = %s", b.Hash)
	}
}

// Rate limited twice then healthy: retries recover and error is classified.
func TestRetryRecoversFromRateLimit(t *testing.T) {
	n, c := newNode(t)
	n.SetFailure("eth_getBlockByNumber", fakenode.FailRateLimit, 2)
	b, err := c.GetBlockByNumber(1)
	if err != nil {
		t.Fatalf("expected recovery, got %v", err)
	}
	if b.Hash != "0xa-1" {
		t.Fatalf("hash = %s", b.Hash)
	}
}

// More failures than retry budget: must fail explicitly, never silently skip.
func TestRetryExhaustedFailsExplicitly(t *testing.T) {
	n, c := newNode(t)
	n.SetFailure("eth_getBlockByNumber", fakenode.FailTemporary, 10)
	_, err := c.GetBlockByNumber(1)
	if err == nil {
		t.Fatal("expected explicit failure after exhausted retries")
	}
	if !errors.Is(err, ErrTemporary) {
		t.Fatalf("want wrapped ErrTemporary for distinguishability, got %v", err)
	}
	if !strings.Contains(err.Error(), "after 4 attempt(s)") {
		t.Fatalf("error should mention attempt count: %v", err)
	}
}

// Timeout is distinguishable from temporary failure.
func TestTimeoutClassifiedDistinctly(t *testing.T) {
	n, _ := newNode(t)
	// client timeout 120ms, node sleeps long for one call then recovers.
	c2 := NewHTTPClient(Config{
		Endpoint: n.URL(), TimeoutMillis: 120, MaxRetries: 0, BackoffMillis: 1,
	})
	defer c2.Close()
	n.SetLatencyMillis(400)
	n.SetFailure("eth_getBlockByNumber", fakenode.FailTimeout, 1)
	_, err := c2.GetBlockByNumber(1)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if errors.Is(err, ErrTemporary) {
		t.Fatalf("timeout must not be classified as temporary: %v", err)
	}
}

// Block missing is non-retryable and immediately classified.
func TestBlockMissingNotRetried(t *testing.T) {
	n, c := newNode(t)
	n.SetFailure("eth_getBlockByNumber", fakenode.FailNotFound, 5)
	_, err := c.GetBlockByNumber(1)
	if !errors.Is(err, ErrBlockMissing) {
		t.Fatalf("want ErrBlockMissing, got %v", err)
	}
	// Non-retryable: exactly one call despite 5 scheduled failures.
	calls := 0
	for _, call := range n.Calls() {
		if call.Method == "eth_getBlockByNumber" {
			calls++
		}
	}
	if calls != 1 {
		t.Fatalf("missing block must not be retried, saw %d calls", calls)
	}
}

// Temporary vs rate-limit vs timeout must be mutually distinguishable.
func TestErrorCausesAreDistinguishable(t *testing.T) {
	n, _ := newNode(t)

	rate := NewHTTPClient(Config{Endpoint: n.URL(), MaxRetries: 0, TimeoutMillis: 1000})
	n.SetFailure("eth_getChainHead", fakenode.FailRateLimit, 1)
	_, err := rate.GetChainHead()
	if !errors.Is(err, ErrRateLimited) || errors.Is(err, ErrTimeout) {
		t.Fatalf("rate limit classification wrong: %v", err)
	}

	temp := NewHTTPClient(Config{Endpoint: n.URL(), MaxRetries: 0, TimeoutMillis: 1000})
	n.SetFailure("eth_getChainHead", fakenode.FailTemporary, 1)
	_, err = temp.GetChainHead()
	if !errors.Is(err, ErrTemporary) || errors.Is(err, ErrRateLimited) {
		t.Fatalf("temporary classification wrong: %v", err)
	}

	var _ = model.Block{}
}
