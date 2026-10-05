package rpc_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
)

// scriptedHTTP replies to every POST with the queued responses in order;
// the last one repeats. Each entry is (status, body).
type scriptedHTTP struct {
	t      *testing.T
	mu     atomic.Int64
	status []int
	body   []string
}

func (s *scriptedHTTP) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := s.mu.Add(1) - 1
		if int(i) >= len(s.status) {
			i = int64(len(s.status)) - 1
		}
		s.t.Logf("http-request=%d -> status=%d body=%q", i+1, s.status[i], s.body[i])
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(s.status[i])
		fmt.Fprint(w, s.body[i])
	})
}

func (s *scriptedHTTP) requests() int { return int(s.mu.Load()) }

func okBody(result string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":%s}`, result)
}

func rpcErrBody(code int, msg string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"error":{"code":%d,"message":%q}}`, code, msg)
}

func newServer(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestJSONRPC_SuccessPath(t *testing.T) {
	sh := &scriptedHTTP{t: t,
		status: []int{200, 200},
		body: []string{
			okBody(`"0x7"`),
			okBody(`{"number":"0x3","hash":"0xaaa","parentHash":"0xbbb","events":[{"Index":0,"SourceHash":"0xaaa","Kind":"Tick","Payload":"A@3"}]}`),
		}}
	c := rpc.NewJSONRPCClient(newServer(t, sh.handler()), time.Second)

	tip, err := c.ChainTip(context.Background())
	if err != nil {
		t.Fatalf("ChainTip: %v", err)
	}
	if tip != 7 {
		t.Fatalf("tip=%d want 7", tip)
	}
	b, err := c.BlockByNumber(context.Background(), 3)
	if err != nil {
		t.Fatalf("BlockByNumber: %v", err)
	}
	if b.Number != 3 || b.Hash != "0xaaa" || b.ParentHash != "0xbbb" || len(b.Events) != 1 {
		t.Fatalf("unexpected block: %+v", b)
	}
	t.Logf("scenario=success tip=%d block=%+v", tip, b)
}

func TestJSONRPC_NullResultIsBlockNotFound(t *testing.T) {
	sh := &scriptedHTTP{t: t, status: []int{200}, body: []string{okBody(`null`)}}
	c := rpc.NewJSONRPCClient(newServer(t, sh.handler()), time.Second)

	_, err := c.BlockByNumber(context.Background(), 9)
	if !errors.Is(err, rpc.ErrBlockNotFound) {
		t.Fatalf("err=%v want ErrBlockNotFound", err)
	}
	if rpc.ClassOf(err) != rpc.ClassPermanent || rpc.IsRetryable(err) {
		t.Fatalf("block-not-found must be permanent/non-retryable, class=%s", rpc.ClassOf(err))
	}
	t.Logf("scenario=null-result class=%s retryable=%v", rpc.ClassOf(err), rpc.IsRetryable(err))
}

func TestJSONRPC_HTTPStatusClassification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   rpc.ErrorClass
	}{
		{"rate-limited-429", 429, "slow down", rpc.ClassRateLimited},
		{"timeout-408", 408, "timeout", rpc.ClassTimeout},
		{"temporary-500", 500, "boom", rpc.ClassTemporary},
		{"temporary-503", 503, "unavailable", rpc.ClassTemporary},
		{"permanent-400", 400, "bad request", rpc.ClassPermanent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sh := &scriptedHTTP{t: t, status: []int{tc.status}, body: []string{tc.body}}
			c := rpc.NewJSONRPCClient(newServer(t, sh.handler()), time.Second)
			_, err := c.ChainTip(context.Background())
			if err == nil {
				t.Fatal("expected error")
			}
			if got := rpc.ClassOf(err); got != tc.want {
				t.Fatalf("class=%s want %s (err=%v)", got, tc.want, err)
			}
			t.Logf("scenario=%s status=%d class=%s retryable=%v",
				tc.name, tc.status, rpc.ClassOf(err), rpc.IsRetryable(err))
		})
	}
}

func TestJSONRPC_RPCErrorCodeClassification(t *testing.T) {
	cases := []struct {
		name string
		code int
		want rpc.ErrorClass
	}{
		{"internal-error--32603", -32603, rpc.ClassTemporary},
		{"server-error--32000", -32000, rpc.ClassTemporary},
		{"server-error--32099", -32099, rpc.ClassTemporary},
		{"method-not-found--32601", -32601, rpc.ClassPermanent},
		{"invalid-params--32602", -32602, rpc.ClassPermanent},
		{"custom-permanent--32800", -32800, rpc.ClassPermanent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sh := &scriptedHTTP{t: t, status: []int{200}, body: []string{rpcErrBody(tc.code, "node says no")}}
			c := rpc.NewJSONRPCClient(newServer(t, sh.handler()), time.Second)
			_, err := c.ChainTip(context.Background())
			if got := rpc.ClassOf(err); got != tc.want {
				t.Fatalf("class=%s want %s (err=%v)", got, tc.want, err)
			}
			t.Logf("scenario=%s code=%d class=%s retryable=%v",
				tc.name, tc.code, rpc.ClassOf(err), rpc.IsRetryable(err))
		})
	}
}

func TestJSONRPC_ClientTimeoutIsClassTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		fmt.Fprint(w, okBody(`"0x1"`))
	}))
	t.Cleanup(srv.Close)

	c := rpc.NewJSONRPCClient(srv.URL, 20*time.Millisecond)
	_, err := c.ChainTip(context.Background())
	if rpc.ClassOf(err) != rpc.ClassTimeout {
		t.Fatalf("class=%s want timeout (err=%v)", rpc.ClassOf(err), err)
	}
	if !rpc.IsRetryable(err) {
		t.Fatal("timeout must be retryable")
	}
	t.Logf("scenario=client-timeout class=%s retryable=%v", rpc.ClassOf(err), rpc.IsRetryable(err))
}

func TestJSONRPC_ConnectionRefusedIsTemporary(t *testing.T) {
	// Bind then close to get an address that refuses connections.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	c := rpc.NewJSONRPCClient(url, time.Second)
	_, err := c.ChainTip(context.Background())
	if rpc.ClassOf(err) != rpc.ClassTemporary {
		t.Fatalf("class=%s want temporary (err=%v)", rpc.ClassOf(err), err)
	}
	t.Logf("scenario=connection-refused class=%s retryable=%v", rpc.ClassOf(err), rpc.IsRetryable(err))
}

func TestJSONRPC_RetryMixedErrorsThenSuccess(t *testing.T) {
	// 429 -> 500 -> success: each class retried, final result delivered.
	sh := &scriptedHTTP{t: t,
		status: []int{429, 500, 200},
		body:   []string{"limited", "boom", okBody(`"0x2a"`)}}
	inner := rpc.NewJSONRPCClient(newServer(t, sh.handler()), time.Second)
	c := rpc.NewRetryClient(inner, rpc.RetryPolicy{
		MaxAttempts: 4, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
	})
	tip, err := c.ChainTip(context.Background())
	if err != nil {
		t.Fatalf("ChainTip: %v", err)
	}
	if tip != 0x2a {
		t.Fatalf("tip=%d want 42", tip)
	}
	if sh.requests() != 3 {
		t.Fatalf("requests=%d want 3", sh.requests())
	}
	t.Logf("scenario=mixed-errors-then-success requests=%d tip=%d", sh.requests(), tip)
}

func TestJSONRPC_RetryExhaustedIsExplicit(t *testing.T) {
	sh := &scriptedHTTP{t: t, status: []int{429}, body: []string{"limited"}}
	inner := rpc.NewJSONRPCClient(newServer(t, sh.handler()), time.Second)
	c := rpc.NewRetryClient(inner, rpc.RetryPolicy{
		MaxAttempts: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
	})
	_, err := c.ChainTip(context.Background())
	if !errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("err=%v want ErrRetryExhausted", err)
	}
	var re *rpc.RetryExhausted
	if !errors.As(err, &re) || re.Attempts != 3 {
		t.Fatalf("attempts not surfaced: %v", err)
	}
	if rpc.ClassOf(re.Last) != rpc.ClassRateLimited {
		t.Fatalf("last error class=%s want rate_limited", rpc.ClassOf(re.Last))
	}
	if sh.requests() != 3 {
		t.Fatalf("requests=%d want exactly 3 (bounded)", sh.requests())
	}
	t.Logf("scenario=retry-exhausted attempts=%d last-class=%s", re.Attempts, rpc.ClassOf(re.Last))
}

func TestJSONRPC_PermanentErrorNotRetried(t *testing.T) {
	sh := &scriptedHTTP{t: t, status: []int{200}, body: []string{rpcErrBody(-32800, "permanent")}}
	inner := rpc.NewJSONRPCClient(newServer(t, sh.handler()), time.Second)
	c := rpc.NewRetryClient(inner, rpc.RetryPolicy{
		MaxAttempts: 5, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond,
	})
	_, err := c.ChainTip(context.Background())
	if err == nil || errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("permanent error must fail immediately, got %v", err)
	}
	if sh.requests() != 1 {
		t.Fatalf("requests=%d want 1 (no retries on permanent)", sh.requests())
	}
	t.Logf("scenario=permanent-no-retry requests=%d err=%v", sh.requests(), err)
}
