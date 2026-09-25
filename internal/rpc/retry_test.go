package rpc_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

type scriptedClient struct {
	blkErrs []error
	calls   int32
	noSleep bool
}

func (c *scriptedClient) BlockByNumber(ctx context.Context, n uint64) (*types.Block, error) {
	i := int(atomic.AddInt32(&c.calls, 1)) - 1
	if i < len(c.blkErrs) && c.blkErrs[i] != nil {
		return nil, c.blkErrs[i]
	}
	return &types.Block{Number: n, Hash: "ok"}, nil
}

func (c *scriptedClient) ChainTip(ctx context.Context) (uint64, error) {
	return 10, nil
}

func newRetryer(inner rpc.Client, attempts int) *rpc.RetryClient {
	rc := rpc.NewRetryClient(inner, rpc.RetryPolicy{
		MaxAttempts: attempts,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	})
	return rc
}

func TestRetry_TimeoutThenSucceeds(t *testing.T) {
	inner := &scriptedClient{blkErrs: []error{
		rpc.NewError(rpc.ClassTimeout, "boom"),
		rpc.NewError(rpc.ClassRateLimited, "429"),
	}}
	rc := newRetryer(inner, 3)
	b, err := rc.BlockByNumber(context.Background(), 5)
	if err != nil {
		t.Fatalf("want success after retries, got %v", err)
	}
	if b.Hash != "ok" {
		t.Fatalf("hash=%s", b.Hash)
	}
	if got := atomic.LoadInt32(&inner.calls); got != 3 {
		t.Fatalf("calls=%d want 3", got)
	}
}

func TestRetry_Exhausted(t *testing.T) {
	inner := &scriptedClient{blkErrs: []error{
		rpc.NewError(rpc.ClassTemporary, "x"),
		rpc.NewError(rpc.ClassTemporary, "x"),
		rpc.NewError(rpc.ClassTemporary, "x"),
	}}
	rc := newRetryer(inner, 3)
	_, err := rc.BlockByNumber(context.Background(), 1)
	if !errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("err=%v want ErrRetryExhausted", err)
	}
	var re *rpc.RetryExhausted
	if !errors.As(err, &re) {
		t.Fatal("expected *RetryExhausted")
	}
	if re.Attempts != 3 {
		t.Fatalf("attempts=%d want 3", re.Attempts)
	}
	if rpc.ClassOf(re.Last) != rpc.ClassTemporary {
		t.Fatalf("last class=%v want temporary", rpc.ClassOf(re.Last))
	}
}

func TestRetry_PermanentNotRetried(t *testing.T) {
	inner := &scriptedClient{blkErrs: []error{
		rpc.NewError(rpc.ClassPermanent, "fatal"),
	}}
	rc := newRetryer(inner, 5)
	_, err := rc.BlockByNumber(context.Background(), 1)
	if err == nil || errors.Is(err, rpc.ErrRetryExhausted) {
		t.Fatalf("permanent error must not be retried: %v", err)
	}
	if got := atomic.LoadInt32(&inner.calls); got != 1 {
		t.Fatalf("calls=%d want 1", got)
	}
}

func TestErrorClassesAreDistinguishable(t *testing.T) {
	cases := map[rpc.ErrorClass]bool{
		rpc.ClassTimeout:     true,
		rpc.ClassTemporary:   true,
		rpc.ClassRateLimited: true,
		rpc.ClassPermanent:   false,
	}
	for class, retryable := range cases {
		err := rpc.NewError(class, "probe")
		if rpc.IsRetryable(err) != retryable {
			t.Fatalf("class=%v retryable=%v want %v", class, !retryable, retryable)
		}
	}
}
