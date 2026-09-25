package rpc

import (
	"errors"
	"fmt"
	"net"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

// Classified transport/node errors. Callers can use errors.Is against them.
var (
	ErrTimeout      = errors.New("rpc: request timeout")
	ErrTemporary    = errors.New("rpc: temporary node failure")
	ErrRateLimited  = errors.New("rpc: rate limited by node")
	ErrBadResponse  = errors.New("rpc: malformed rpc response")
	ErrBlockMissing = errors.New("rpc: block not found at height")
)

// Client is the minimal JSON-RPC surface used by the indexer.
type Client interface {
	GetBlockByNumber(height uint64) (*model.Block, error)
	GetChainHead() (*model.ChainHead, error)
	Close()
}

// ClassifyError maps a transport-level error to one of the sentinel errors.
// Already-classified errors are returned unchanged.
func ClassifyError(err error) error {
	if err == nil {
		return nil
	}
	// Client-side deadline (per-request context or http.Client timeout).
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%w: %v", ErrTimeout, err)
	}
	// Connection refused / reset / DNS: transient, retryable.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return fmt.Errorf("%w: %v", ErrTemporary, err)
	}
	return err
}
