package fakenode

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

const (
	methodGetBlock  = "eth_getBlockByNumber"
	methodChainHead = "eth_getChainHead"
)

// JSON-RPC 2.0 error codes used by the fake node. They mirror the codes
// the real client knows how to classify.
const (
	codeTemporary    = -32603
	codeBlockMissing = -32001
	codeRateLimited  = -32005
)

type rpcRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type headWire struct {
	Number uint64 `json:"number"`
	Hash   string `json:"hash"`
}

// httpHandler builds the JSON-RPC 2.0 HTTP handler.
func httpHandler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "post only", http.StatusMethodNotAllowed)
			return
		}
		var req rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeRPC(w, nil, &rpcError{Code: -32700, Message: "parse error"})
			return
		}

		var height uint64
		if len(req.Params) > 0 {
			var h uint64
			if err := json.Unmarshal(req.Params[0], &h); err == nil {
				height = h
			}
		}

		n.mu.Lock()
		latency := n.latencyMS
		mode := n.consumeFailureLocked(req.Method)
		n.calls = append(n.calls, Call{Method: req.Method, Height: height})
		n.mu.Unlock()

		if latency > 0 {
			time.Sleep(time.Duration(latency) * time.Millisecond)
		}

		switch mode {
		case FailTimeout:
			// Exceed any sane client deadline; the client should give up
			// and classify this as ErrTimeout.
			time.Sleep(2 * time.Second)
		case FailTemporary:
			writeRPC(w, req.ID, &rpcError{Code: codeTemporary, Message: "temporary node failure"})
			return
		case FailRateLimit:
			w.Header().Set("Retry-After", "1")
			writeRPCStatus(w, http.StatusTooManyRequests, req.ID,
				&rpcError{Code: codeRateLimited, Message: "rate limited"})
			return
		case FailNotFound:
			writeRPC(w, req.ID, &rpcError{Code: codeBlockMissing, Message: "block not found"})
			return
		case FailureMode(FailureSkipHeight):
			// Serve the next height's block: simulates a node that skips
			// the requested height / serves inconsistent numbering.
			skip := height + 1
			b := n.canonicalBlock(skip)
			if b == nil {
				writeRPC(w, req.ID, &rpcError{Code: codeBlockMissing, Message: "block not found"})
				return
			}
			writeRPC(w, req.ID, toWire(b))
			return
		}

		switch req.Method {
		case methodGetBlock:
			b := n.canonicalBlock(height)
			if b == nil {
				writeRPC(w, req.ID, &rpcError{Code: codeBlockMissing, Message: "block not found"})
				return
			}
			writeRPC(w, req.ID, toWire(b))
		case methodChainHead:
			h, hash := n.canonicalTip()
			if hash == "" {
				writeRPC(w, req.ID, &rpcError{Code: codeTemporary, Message: "chain not ready"})
				return
			}
			writeRPC(w, req.ID, &headWire{Number: h, Hash: hash})
		default:
			writeRPC(w, req.ID, &rpcError{Code: -32601, Message: "method not found"})
		}
	})
	return mux
}

// consumeFailureLocked returns and decrements a scheduled failure for method.
// Caller must hold n.mu.
func (n *Node) consumeFailureLocked(method string) FailureMode {
	rule := n.failures[method]
	if rule == nil || rule.times <= 0 {
		return FailNone
	}
	rule.times--
	mode := rule.mode
	if rule.times == 0 {
		delete(n.failures, method)
	}
	return mode
}

func (n *Node) canonicalBlock(height uint64) *model.Block {
	n.mu.Lock()
	defer n.mu.Unlock()
	hash, ok := n.canonical[height]
	if !ok {
		return nil
	}
	return n.pool[hash]
}

func (n *Node) canonicalTip() (uint64, string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var h uint64
	found := false
	for k := range n.canonical {
		if !found || k > h {
			h = k
			found = true
		}
	}
	if !found {
		return 0, ""
	}
	return h, n.canonical[h]
}

func writeRPC(w http.ResponseWriter, id json.RawMessage, resultOrErr any) {
	resp := rpcResponse{JSONRPC: "2.0", ID: id}
	if e, ok := resultOrErr.(*rpcError); ok {
		resp.Error = e
	} else {
		resp.Result = resultOrErr
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func writeRPCStatus(w http.ResponseWriter, status int, id json.RawMessage, e *rpcError) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(rpcResponse{JSONRPC: "2.0", ID: id, Error: e})
}
