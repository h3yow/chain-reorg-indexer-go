package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// Wire method names served by standard nodes (eth-style).
const (
	methodBlockNumber   = "eth_blockNumber"
	methodGetBlockByNum = "eth_getBlockByNumber"
)

// JSONRPCClient talks to a standard chain node over HTTP JSON-RPC.
//
// Error classification (consumed by RetryClient / IsRetryable):
//   - connection refused / DNS / other transport errors -> ClassTemporary
//   - client timeout / context deadline / HTTP 408      -> ClassTimeout
//   - HTTP 429                                          -> ClassRateLimited
//   - other HTTP 5xx                                    -> ClassTemporary
//   - other HTTP non-2xx                                -> ClassPermanent
//   - JSON-RPC error codes: -32603 and -32000..-32099   -> ClassTemporary
//   - other JSON-RPC error codes                        -> ClassPermanent
//   - null result from eth_getBlockByNumber             -> ErrBlockNotFound
type JSONRPCClient struct {
	endpoint string
	hc       *http.Client
	ids      atomic.Uint64
}

// NewJSONRPCClient builds a client for endpoint (e.g. http://127.0.0.1:8545).
// timeout bounds each individual HTTP round trip; 0 means no client-side
// timeout (context cancellation still applies).
func NewJSONRPCClient(endpoint string, timeout time.Duration) *JSONRPCClient {
	return &JSONRPCClient{endpoint: endpoint, hc: &http.Client{Timeout: timeout}}
}

var _ Client = (*JSONRPCClient)(nil)

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// blockWire is the JSON shape of a block on the wire.
type blockWire struct {
	Number     string        `json:"number"`
	Hash       string        `json:"hash"`
	ParentHash string        `json:"parentHash"`
	Events     []types.Event `json:"events"`
}

// BlockByNumber implements Client via eth_getBlockByNumber.
func (c *JSONRPCClient) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	hexNum := "0x" + strconv.FormatUint(number, 16)
	res, err := c.call(ctx, methodGetBlockByNum, []any{hexNum, false})
	if err != nil {
		return nil, err
	}
	if len(res) == 0 || string(res) == "null" {
		return nil, &BlockNotFoundError{Height: number}
	}
	var bw blockWire
	if err := json.Unmarshal(res, &bw); err != nil {
		return nil, NewError(ClassPermanent, "malformed block at height %d: %v", number, err)
	}
	n, err := parseHexUint(bw.Number)
	if err != nil {
		return nil, NewError(ClassPermanent, "malformed block number %q: %v", bw.Number, err)
	}
	events := bw.Events
	if events == nil {
		events = []types.Event{}
	}
	return &types.Block{
		Number:     n,
		Hash:       bw.Hash,
		ParentHash: bw.ParentHash,
		Events:     events,
	}, nil
}

// ChainTip implements Client via eth_blockNumber.
func (c *JSONRPCClient) ChainTip(ctx context.Context) (uint64, error) {
	res, err := c.call(ctx, methodBlockNumber, []any{})
	if err != nil {
		return 0, err
	}
	var hexTip string
	if err := json.Unmarshal(res, &hexTip); err != nil {
		return 0, NewError(ClassPermanent, "malformed blockNumber result: %v", err)
	}
	tip, err := parseHexUint(hexTip)
	if err != nil {
		return 0, NewError(ClassPermanent, "malformed blockNumber %q: %v", hexTip, err)
	}
	return tip, nil
}

// call performs one JSON-RPC round trip and classifies every failure mode.
func (c *JSONRPCClient) call(ctx context.Context, method string, params []any) (json.RawMessage, error) {
	reqBody, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      c.ids.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, NewError(ClassPermanent, "encode request: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return nil, NewError(ClassPermanent, "build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, classifyTransportError(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, classifyTransportError(err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, classifyHTTPStatus(resp.StatusCode, body)
	}

	var rr rpcResponse
	if err := json.Unmarshal(body, &rr); err != nil {
		return nil, NewError(ClassPermanent, "malformed JSON-RPC response: %v", err)
	}
	if rr.Error != nil {
		return nil, classifyRPCError(rr.Error)
	}
	return rr.Result, nil
}

// classifyTransportError maps network-layer failures to error classes.
func classifyTransportError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return NewError(ClassTimeout, "request timed out: %v", err)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return NewError(ClassTimeout, "request timed out: %v", err)
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	// Connection refused, DNS failures, resets: transient by default.
	return NewError(ClassTemporary, "transport error: %v", err)
}

// classifyHTTPStatus maps HTTP status codes to error classes.
func classifyHTTPStatus(code int, body []byte) error {
	snippet := strings.TrimSpace(string(body))
	if len(snippet) > 120 {
		snippet = snippet[:120] + "..."
	}
	msg := fmt.Sprintf("HTTP %d from node", code)
	if snippet != "" {
		msg += ": " + snippet
	}
	switch {
	case code == http.StatusTooManyRequests:
		return NewError(ClassRateLimited, "%s", msg)
	case code == http.StatusRequestTimeout:
		return NewError(ClassTimeout, "%s", msg)
	case code >= 500 && code <= 599:
		return NewError(ClassTemporary, "%s", msg)
	default:
		return NewError(ClassPermanent, "%s", msg)
	}
}

// classifyRPCError maps JSON-RPC error codes to error classes.
func classifyRPCError(re *rpcError) error {
	class := ClassPermanent
	switch {
	case re.Code == -32603: // internal error: worth retrying
		class = ClassTemporary
	case re.Code >= -32099 && re.Code <= -32000: // server error range
		class = ClassTemporary
	}
	return NewError(class, "JSON-RPC error %d: %s", re.Code, re.Message)
}

func parseHexUint(s string) (uint64, error) {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0, errors.New("empty hex quantity")
	}
	return strconv.ParseUint(s, 16, 64)
}
