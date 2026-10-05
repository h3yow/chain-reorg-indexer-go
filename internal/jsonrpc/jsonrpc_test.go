package jsonrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/fakenode"
	"github.com/h3yow/chain-reorg-indexer-go/internal/jsonrpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
)

// serveNode starts a local HTTP JSON-RPC node backed by the fake node.
func serveNode(t *testing.T, n *fakenode.Node) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(jsonrpc.NewHandler(n))
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTP_SuccessPath_MatchesInProcess(t *testing.T) {
	node := fakenode.New()
	node.SetChain(fakenode.BuildLinear("A", 0, 8)...)
	srv := serveNode(t, node)

	ctx := context.Background()
	direct := rpc.Client(node)
	remote := rpc.Client(jsonrpc.NewHTTPClient(srv.URL))

	tipDirect, err := direct.ChainTip(ctx)
	if err != nil {
		t.Fatalf("direct tip: %v", err)
	}
	tipRemote, err := remote.ChainTip(ctx)
	if err != nil {
		t.Fatalf("remote tip: %v", err)
	}
	if tipDirect != tipRemote {
		t.Fatalf("tip mismatch: direct=%d remote=%d", tipDirect, tipRemote)
	}
	t.Logf("scenario=http-success tip=%d (direct == remote)", tipRemote)

	for h := uint64(0); h <= tipRemote; h++ {
		want, err := direct.BlockByNumber(ctx, h)
		if err != nil {
			t.Fatalf("direct block %d: %v", h, err)
		}
		got, err := remote.BlockByNumber(ctx, h)
		if err != nil {
			t.Fatalf("remote block %d: %v", h, err)
		}
		if want.Hash != got.Hash || want.Number != got.Number ||
			want.ParentHash != got.ParentHash || len(want.Events) != len(got.Events) {
			t.Fatalf("block %d mismatch: direct=%+v remote=%+v", h, want, got)
		}
		if len(want.Events) > 0 && want.Events[0] != got.Events[0] {
			t.Fatalf("block %d event mismatch: %+v vs %+v", h, want.Events[0], got.Events[0])
		}
	}
	t.Logf("scenario=http-success blocks 0..%d identical over HTTP and in-process", tipRemote)
}

func TestHTTP_BlockNotFound_IsPermanentAndTyped(t *testing.T) {
	node := fakenode.New()
	node.SetChain(fakenode.BuildLinear("A", 0, 3)...)
	srv := serveNode(t, node)

	_, err := jsonrpc.NewHTTPClient(srv.URL).BlockByNumber(context.Background(), 99)
	if !errors.Is(err, rpc.ErrBlockNotFound) {
		t.Fatalf("want ErrBlockNotFound, got %v", err)
	}
	var nf *rpc.BlockNotFoundError
	if !errors.As(err, &nf) || nf.Height != 99 {
		t.Fatalf("want BlockNotFoundError height=99, got %v", err)
	}
	if rpc.IsRetryable(err) {
		t.Fatalf("block-not-found must not be retryable")
	}
	t.Logf("scenario=jsonrpc-code -32001 -> ErrBlockNotFound (permanent, height preserved): %v", err)
}

// scriptedServer returns the same HTTP status for every request, counting hits.
func scriptedServer(t *testing.T, status int, body string, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTP_ErrorClassification(t *testing.T) {
	ctx := context.Background()

	t.Run("http-429 -> rate limited, retryable", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusTooManyRequests, `slow down`, &hits)
		_, err := jsonrpc.NewHTTPClient(srv.URL).ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassRateLimited || !rpc.IsRetryable(err) {
			t.Fatalf("want rate_limited+retryable, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=http-status 429 -> class=%s retryable=true err=%v", rpc.ClassOf(err), err)
	})

	t.Run("http-500 -> temporary, retryable", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusInternalServerError, `boom`, &hits)
		_, err := jsonrpc.NewHTTPClient(srv.URL).ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassTemporary || !rpc.IsRetryable(err) {
			t.Fatalf("want temporary+retryable, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=http-status 500 -> class=%s retryable=true err=%v", rpc.ClassOf(err), err)
	})

	t.Run("http-400 -> permanent, not retryable", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusBadRequest, `bad`, &hits)
		_, err := jsonrpc.NewHTTPClient(srv.URL).ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassPermanent || rpc.IsRetryable(err) {
			t.Fatalf("want permanent+not-retryable, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=http-status 400 -> class=%s retryable=false err=%v", rpc.ClassOf(err), err)
	})

	t.Run("jsonrpc -32601 method not found -> permanent", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusOK,
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"no such method"}}`, &hits)
		_, err := jsonrpc.NewHTTPClient(srv.URL).ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassPermanent || rpc.IsRetryable(err) {
			t.Fatalf("want permanent, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=jsonrpc-code -32601 -> class=%s retryable=false err=%v", rpc.ClassOf(err), err)
	})

	t.Run("jsonrpc -32603 internal -> temporary", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusOK,
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"db locked"}}`, &hits)
		_, err := jsonrpc.NewHTTPClient(srv.URL).ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassTemporary || !rpc.IsRetryable(err) {
			t.Fatalf("want temporary, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=jsonrpc-code -32603 -> class=%s retryable=true err=%v", rpc.ClassOf(err), err)
	})

	t.Run("jsonrpc -32005 -> rate limited", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusOK,
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"limit exceeded"}}`, &hits)
		_, err := jsonrpc.NewHTTPClient(srv.URL).ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassRateLimited || !rpc.IsRetryable(err) {
			t.Fatalf("want rate_limited, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=jsonrpc-code -32005 -> class=%s retryable=true err=%v", rpc.ClassOf(err), err)
	})

	t.Run("jsonrpc -32004 -> timeout", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusOK,
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"upstream timeout"}}`, &hits)
		_, err := jsonrpc.NewHTTPClient(srv.URL).ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassTimeout || !rpc.IsRetryable(err) {
			t.Fatalf("want timeout, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=jsonrpc-code -32004 -> class=%s retryable=true err=%v", rpc.ClassOf(err), err)
	})

	t.Run("connection refused -> temporary", func(t *testing.T) {
		// Bind and immediately close to get an address that refuses connections.
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		addr := srv.URL
		srv.Close()
		_, err := jsonrpc.NewHTTPClient(addr).ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassTemporary || !rpc.IsRetryable(err) {
			t.Fatalf("want temporary+retryable, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=connection-refused -> class=%s retryable=true err=%v", rpc.ClassOf(err), err)
	})

	t.Run("deadline exceeded -> timeout", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		_, err := jsonrpc.NewHTTPClient(srv.URL).ChainTip(cctx)
		if rpc.ClassOf(err) != rpc.ClassTimeout || !rpc.IsRetryable(err) {
			t.Fatalf("want timeout+retryable, got class=%s err=%v", rpc.ClassOf(err), err)
		}
		t.Logf("scenario=context-deadline -> class=%s retryable=true err=%v", rpc.ClassOf(err), err)
	})
}

func TestHTTP_RetryThenSuccess_And_Exhaustion(t *testing.T) {
	ctx := context.Background()
	policy := rpc.RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}

	t.Run("transient 429 recovers within attempts", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hits.Add(1) < 2 { // first attempt limited, second succeeds
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0", "id": 1, "result": "0x7",
			})
		}))
		defer srv.Close()

		client := rpc.NewRetryClient(jsonrpc.NewHTTPClient(srv.URL), policy)
		tip, err := client.ChainTip(ctx)
		if err != nil {
			t.Fatalf("want success after retry, got %v", err)
		}
		if tip != 7 {
			t.Fatalf("want tip 7, got %d", tip)
		}
		if hits.Load() != 2 {
			t.Fatalf("want 2 attempts, got %d", hits.Load())
		}
		t.Logf("scenario=retry-recover attempts=%d tip=%d (429 then 200)", hits.Load(), tip)
	})

	t.Run("persistent 429 exhausts attempts explicitly", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusTooManyRequests, `limited`, &hits)
		client := rpc.NewRetryClient(jsonrpc.NewHTTPClient(srv.URL), policy)
		_, err := client.ChainTip(ctx)
		if !errors.Is(err, rpc.ErrRetryExhausted) {
			t.Fatalf("want ErrRetryExhausted, got %v", err)
		}
		var exhausted *rpc.RetryExhausted
		if !errors.As(err, &exhausted) || exhausted.Attempts != 3 {
			t.Fatalf("want 3 attempts recorded, got %v", err)
		}
		if rpc.ClassOf(exhausted.Last) != rpc.ClassRateLimited {
			t.Fatalf("last error class want rate_limited, got %s", rpc.ClassOf(exhausted.Last))
		}
		if hits.Load() != 3 {
			t.Fatalf("want exactly 3 HTTP calls, got %d", hits.Load())
		}
		t.Logf("scenario=retry-exhausted attempts=%d last-class=%s err=%v",
			exhausted.Attempts, rpc.ClassOf(exhausted.Last), err)
	})

	t.Run("permanent error fails immediately without retry", func(t *testing.T) {
		var hits atomic.Int32
		srv := scriptedServer(t, http.StatusBadRequest, `nope`, &hits)
		client := rpc.NewRetryClient(jsonrpc.NewHTTPClient(srv.URL), policy)
		_, err := client.ChainTip(ctx)
		if err == nil || rpc.ClassOf(err) != rpc.ClassPermanent {
			t.Fatalf("want immediate permanent failure, got %v", err)
		}
		if errors.Is(err, rpc.ErrRetryExhausted) {
			t.Fatalf("permanent error must not be reported as exhaustion: %v", err)
		}
		if hits.Load() != 1 {
			t.Fatalf("want exactly 1 HTTP call, got %d", hits.Load())
		}
		t.Logf("scenario=permanent-no-retry attempts=%d err=%v", hits.Load(), err)
	})

	t.Run("mixed failures keep their own classes", func(t *testing.T) {
		// Sequence: 429, 500, then 400 -> the final permanent error must
		// surface as permanent, and each earlier class must have been
		// treated as retryable (proven by the attempts happening at all).
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch hits.Add(1) {
			case 1:
				w.WriteHeader(http.StatusTooManyRequests)
			case 2:
				w.WriteHeader(http.StatusInternalServerError)
			default:
				w.WriteHeader(http.StatusBadRequest)
			}
		}))
		defer srv.Close()
		client := rpc.NewRetryClient(jsonrpc.NewHTTPClient(srv.URL), policy)
		_, err := client.ChainTip(ctx)
		if rpc.ClassOf(err) != rpc.ClassPermanent {
			t.Fatalf("want final class permanent, got %s (%v)", rpc.ClassOf(err), err)
		}
		if hits.Load() != 3 {
			t.Fatalf("want 3 attempts (429,500 retried; 400 terminal), got %d", hits.Load())
		}
		t.Logf("scenario=mixed-failures attempts=%d sequence=429,500,400 final-class=%s",
			hits.Load(), rpc.ClassOf(err))
	})
}
