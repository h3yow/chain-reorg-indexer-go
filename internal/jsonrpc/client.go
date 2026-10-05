package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// HTTPClient is an rpc.Client that talks to a node over JSON-RPC 2.0 / HTTP.
// Transport and protocol failures are mapped onto the shared rpc error
// classes so the retry layer treats in-process and networked nodes alike:
//
//	connection refused / DNS / reset      -> ClassTemporary  (retryable)
//	deadline exceeded / net timeout       -> ClassTimeout    (retryable)
//	HTTP 429                              -> ClassRateLimited (retryable)
//	HTTP 5xx                              -> ClassTemporary  (retryable)
//	other HTTP 4xx                        -> ClassPermanent
//	JSON-RPC -32001                       -> rpc.ErrBlockNotFound (permanent)
//	JSON-RPC -32004                       -> ClassTimeout
//	JSON-RPC -32005                       -> ClassRateLimited
//	JSON-RPC -32603 / -32000              -> ClassTemporary
//	JSON-RPC -32700/-32600/-32601/-32602  -> ClassPermanent
type HTTPClient struct {
	endpoint string
	hc       *http.Client
	nextID   atomic.Int64
}

// ClientOption customizes an HTTPClient.
type ClientOption func(*HTTPClient)

// WithHTTPClient overrides the underlying http.Client (e.g. for timeouts).
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *HTTPClient) { c.hc = hc }
}

// NewHTTPClient builds a client for endpoint (e.g. "http://127.0.0.1:8545").
func NewHTTPClient(endpoint string, opts ...ClientOption) *HTTPClient {
	c := &HTTPClient{
		endpoint: endpoint,
		hc:       &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

var _ rpc.Client = (*HTTPClient)(nil)

// ChainTip implements rpc.Client.
func (c *HTTPClient) ChainTip(ctx context.Context) (uint64, error) {
	raw, err := c.call(ctx, MethodChainTip, nil)
	if err != nil {
		return 0, err
	}
	var hexTip string
	if err := json.Unmarshal(raw, &hexTip); err != nil {
		return 0, rpc.NewError(rpc.ClassPermanent, "chain tip: malformed result: %v", err)
	}
	tip, err := decodeUint(hexTip)
	if err != nil {
		return 0, rpc.NewError(rpc.ClassPermanent, "chain tip: bad quantity %q", hexTip)
	}
	return tip, nil
}

// BlockByNumber implements rpc.Client.
func (c *HTTPClient) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	raw, err := c.call(ctx, MethodGetBlockByNum, []string{encodeUint(number)})
	if err != nil {
		if errors.Is(err, rpc.ErrBlockNotFound) {
			// Re-wrap with the requested height so callers see which block
			// is missing (the wire error does not carry it).
			return nil, &rpc.BlockNotFoundError{Height: number}
		}
		return nil, err
	}
	var dto blockDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return nil, rpc.NewError(rpc.ClassPermanent, "block %d: malformed result: %v", number, err)
	}
	return blockFromDTO(dto), nil
}

// call performs one JSON-RPC round trip and classifies every failure mode.
func (c *HTTPClient) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := c.nextID.Add(1)
	reqBody, err := json.Marshal(Request{JSONRPC: "2.0", ID: id, Method: method, Params: mustJSON(params)})
	if err != nil {
		return nil, rpc.NewError(rpc.ClassPermanent, "encode request: %v", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, rpc.NewError(rpc.ClassPermanent, "build request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return nil, classifyTransportError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, classifyHTTPStatus(resp.StatusCode, string(body))
	}

	var rpcResp Response
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, rpc.NewError(rpc.ClassTemporary, "decode response: %v", err)
	}
	if rpcResp.Error != nil {
		return nil, classifyRPCError(rpcResp.Error, method)
	}
	if rpcResp.Result == nil {
		return nil, rpc.NewError(rpc.ClassTemporary, "%s: response has neither result nor error", method)
	}
	return rpcResp.Result, nil
}

// classifyTransportError maps net/http transport failures to error classes.
func classifyTransportError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return rpc.NewError(rpc.ClassTimeout, "request deadline exceeded: %v", err)
	}
	if errors.Is(err, context.Canceled) {
		return err // caller cancellation is not a node failure
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return rpc.NewError(rpc.ClassTimeout, "network timeout: %v", err)
	}
	// Connection refused/reset/DNS: the node may recover, so it is temporary.
	return rpc.NewError(rpc.ClassTemporary, "transport error: %v", err)
}

// classifyHTTPStatus maps non-200 HTTP statuses to error classes.
func classifyHTTPStatus(status int, body string) error {
	switch {
	case status == http.StatusTooManyRequests:
		return rpc.NewError(rpc.ClassRateLimited, "HTTP 429 rate limited: %s", trim(body))
	case status == http.StatusRequestTimeout:
		return rpc.NewError(rpc.ClassTimeout, "HTTP 408 request timeout")
	case status >= 500 && status <= 599:
		return rpc.NewError(rpc.ClassTemporary, "HTTP %d server error: %s", status, trim(body))
	default:
		return rpc.NewError(rpc.ClassPermanent, "HTTP %d: %s", status, trim(body))
	}
}

// classifyRPCError maps JSON-RPC error codes to error classes.
func classifyRPCError(e *ErrorObject, method string) error {
	msg := fmt.Sprintf("%s: node error %d: %s", method, e.Code, e.Message)
	switch e.Code {
	case CodeBlockNotFound:
		// Not retryable (unknown class is never retried); BlockByNumber
		// re-wraps this with the requested height.
		return fmt.Errorf("%w: %s", rpc.ErrBlockNotFound, msg)
	case CodeTimeout:
		return rpc.NewError(rpc.ClassTimeout, "%s", msg)
	case CodeRateLimited:
		return rpc.NewError(rpc.ClassRateLimited, "%s", msg)
	case CodeInternal, CodeServerError:
		return rpc.NewError(rpc.ClassTemporary, "%s", msg)
	default:
		// -32700/-32600/-32601/-32602 and unknown codes: retrying the same
		// request cannot help.
		return rpc.NewError(rpc.ClassPermanent, "%s", msg)
	}
}

func trim(s string) string {
	const max = 120
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}
