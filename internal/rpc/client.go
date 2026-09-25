package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

// Config configures the JSON-RPC client.
type Config struct {
	// Endpoint is the HTTP JSON-RPC URL.
	Endpoint string
	// TimeoutMillis bounds a single HTTP attempt. 0 = default 3s.
	TimeoutMillis int
	// MaxRetries is the number of *retries* after the first attempt
	// (so total attempts = MaxRetries+1).
	MaxRetries int
	// BackoffMillis is the base exponential backoff: delay after attempt i
	// is BackoffMillis * 2^i.
	BackoffMillis int
}

// HTTPClient is a JSON-RPC 2.0 over HTTP client with bounded retry/backoff.
type HTTPClient struct {
	cfg  Config
	http *http.Client
}

// NewHTTPClient constructs a client.
func NewHTTPClient(cfg Config) *HTTPClient {
	if cfg.TimeoutMillis <= 0 {
		cfg.TimeoutMillis = 3000
	}
	if cfg.BackoffMillis < 0 {
		cfg.BackoffMillis = 0
	}
	return &HTTPClient{
		cfg:  cfg,
		http: &http.Client{Timeout: time.Duration(cfg.TimeoutMillis) * time.Millisecond},
	}
}

type wireBlock struct {
	Number     uint64      `json:"number"`
	Hash       string      `json:"hash"`
	ParentHash string      `json:"parentHash"`
	Events     []wireEvent `json:"events"`
}

type wireEvent struct {
	Index   int    `json:"index"`
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
}

type wireHead struct {
	Number uint64 `json:"number"`
	Hash   string `json:"hash"`
}

// GetBlockByNumber fetches one canonical block.
func (c *HTTPClient) GetBlockByNumber(height uint64) (*model.Block, error) {
	var out wireBlock
	if err := c.call(methodGetBlockByNumber, []any{height}, &out); err != nil {
		return nil, err
	}
	b := &model.Block{Number: out.Number, Hash: out.Hash, ParentHash: out.ParentHash}
	for _, e := range out.Events {
		b.Events = append(b.Events, model.Event{
			Index:       e.Index,
			BlockNumber: b.Number,
			BlockHash:   b.Hash,
			Kind:        e.Kind,
			Payload:     e.Payload,
		})
	}
	return b, nil
}

// GetChainHead fetches the chain tip.
func (c *HTTPClient) GetChainHead() (*model.ChainHead, error) {
	var out wireHead
	if err := c.call(methodGetChainHead, nil, &out); err != nil {
		return nil, err
	}
	return &model.ChainHead{Number: out.Number, Hash: out.Hash}, nil
}

// Close releases idle connections.
func (c *HTTPClient) Close() {
	c.http.CloseIdleConnections()
}

const (
	methodGetBlockByNumber = "eth_getBlockByNumber"
	methodGetChainHead     = "eth_getChainHead"

	codeTemporary    = -32603
	codeBlockMissing = -32001
	codeRateLimited  = -32005
)

type jsonrpcReq struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type jsonrpcResp struct {
	Result json.RawMessage `json:"result"`
	Error  *jsonrpcError   `json:"error"`
}

type jsonrpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// call performs one JSON-RPC method with bounded retries. Retryable failures
// (timeout / temporary / rate limit) are retried up to cfg.MaxRetries with
// exponential backoff; the budget exhausted error wraps the classified
// failure so callers can still errors.Is the underlying cause.
func (c *HTTPClient) call(method string, params []any, out any) error {
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(c.cfg.BackoffMillis) * time.Millisecond *
				time.Duration(1<<uint(attempt-1)))
		}
		raw, classified, err := c.once(context.Background(), method, params)
		if err == nil {
			if jerr := json.Unmarshal(raw, out); jerr != nil {
				return fmt.Errorf("%w: %v", ErrBadResponse, jerr)
			}
			return nil
		}
		lastErr = err
		if !retryable(classified) {
			return err
		}
	}
	return fmt.Errorf("rpc: %s failed after %d attempt(s): %w",
		method, c.cfg.MaxRetries+1, lastErr)
}

func retryable(err error) bool {
	return errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrTemporary) ||
		errors.Is(err, ErrRateLimited)
}

// once executes a single HTTP attempt and returns (raw result, classified, error).
func (c *HTTPClient) once(ctx context.Context, method string, params []any) (json.RawMessage, error, error) {
	body, err := json.Marshal(jsonrpcReq{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrBadResponse, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		classified := ClassifyError(err)
		return nil, classified, classified
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, ErrBadResponse, fmt.Errorf("%w: read body: %v", ErrBadResponse, err)
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		e := fmt.Errorf("%w: http 429", ErrRateLimited)
		return nil, e, e
	}

	var jr jsonrpcResp
	if err := json.Unmarshal(data, &jr); err != nil {
		e := fmt.Errorf("%w: status %d: %s", ErrBadResponse, resp.StatusCode, truncate(data))
		return nil, e, e
	}
	if jr.Error != nil {
		e := classifyRPCError(jr.Error.Code, jr.Error.Message)
		return nil, e, e
	}
	return jr.Result, nil, nil
}

func classifyRPCError(code int, msg string) error {
	switch code {
	case codeBlockMissing:
		return fmt.Errorf("%w: %s", ErrBlockMissing, msg)
	case codeRateLimited:
		return fmt.Errorf("%w: %s", ErrRateLimited, msg)
	default:
		return fmt.Errorf("%w: code=%d %s", ErrTemporary, code, msg)
	}
}

func truncate(b []byte) string {
	const n = 120
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
