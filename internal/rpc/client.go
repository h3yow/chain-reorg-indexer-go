package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// Client is the block-reading interface of a chain node.
type Client interface {
	// BlockByNumber returns the block at the given height.
	BlockByNumber(ctx context.Context, number uint64) (*types.Block, error)
	// ChainTip returns the newest block height the node knows about.
	ChainTip(ctx context.Context) (uint64, error)
}

// RetryPolicy bounds retry behavior against retryable node errors.
type RetryPolicy struct {
	MaxAttempts int           // total attempts including the first
	BaseBackoff time.Duration // first backoff; doubles each attempt
	MaxBackoff  time.Duration
}

// DefaultRetryPolicy returns a policy with sane defaults.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseBackoff: 10 * time.Millisecond, MaxBackoff: time.Second}
}

// ErrRetryExhausted wraps the last error once MaxAttempts is exceeded.
var ErrRetryExhausted = errors.New("rpc: retry attempts exhausted")

// RetryExhausted pairs ErrRetryExhausted with the last node error and the
// number of attempts made.
type RetryExhausted struct {
	Attempts int
	Last     error
}

func (e *RetryExhausted) Error() string {
	return fmt.Sprintf("%v after %d attempt(s): %v", ErrRetryExhausted, e.Attempts, e.Last)
}

func (e *RetryExhausted) Unwrap() error { return e.Last }

// Is allows errors.Is(err, ErrRetryExhausted).
func (e *RetryExhausted) Is(target error) bool { return target == ErrRetryExhausted }

// RetryClient wraps a Client and retries retryable errors per RetryPolicy.
// It is safe for concurrent use (the indexer fetches blocks in parallel).
type RetryClient struct {
	inner  Client
	policy RetryPolicy
	mu     sync.Mutex // guards rng (rand.Rand is not goroutine-safe)
	rng    *rand.Rand
	sleep  func(ctx context.Context, d time.Duration) error
}

// NewRetryClient wraps inner with bounded retries and exponential backoff.
func NewRetryClient(inner Client, policy RetryPolicy) *RetryClient {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	return &RetryClient{
		inner:  inner,
		policy: policy,
		rng:    rand.New(rand.NewSource(1)),
		sleep:  sleepCtx,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var _ Client = (*RetryClient)(nil)

// call runs fn with bounded retry. Non-retryable errors return immediately;
// exceeding MaxAttempts returns *RetryExhausted instead of silently skipping.
func (c *RetryClient) call(ctx context.Context, what string, fn func() error) error {
	var last error
	for attempt := 1; attempt <= c.policy.MaxAttempts; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		last = err
		if !IsRetryable(err) || attempt == c.policy.MaxAttempts {
			break
		}
		backoff := c.policy.BaseBackoff << (attempt - 1)
		if backoff <= 0 || backoff > c.policy.MaxBackoff {
			backoff = c.policy.MaxBackoff
		}
		// 25% full jitter to avoid thundering-herd retries.
		c.mu.Lock()
		jitter := c.rng.Int63n(int64(backoff)/4 + 1)
		c.mu.Unlock()
		wait := backoff - time.Duration(jitter)
		if wait < 0 {
			wait = backoff
		}
		if err := c.sleep(ctx, wait); err != nil {
			return err
		}
	}
	if IsRetryable(last) {
		return &RetryExhausted{Attempts: c.policy.MaxAttempts, Last: last}
	}
	return last
}

// BlockByNumber delegates to the inner client with retries.
func (c *RetryClient) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	var out *types.Block
	err := c.call(ctx, fmt.Sprintf("block %d", number), func() error {
		b, err := c.inner.BlockByNumber(ctx, number)
		if err != nil {
			return err
		}
		out = b
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ChainTip delegates to the inner client with retries.
func (c *RetryClient) ChainTip(ctx context.Context) (uint64, error) {
	var tip uint64
	err := c.call(ctx, "chain tip", func() error {
		v, err := c.inner.ChainTip(ctx)
		if err != nil {
			return err
		}
		tip = v
		return nil
	})
	if err != nil {
		return 0, err
	}
	return tip, nil
}
