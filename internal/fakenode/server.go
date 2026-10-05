package fakenode

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/h3yow/chain-reorg-indexer-go/internal/rpc"
	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// rpcPermanentCode is the JSON-RPC error code the fake server uses for
// permanent node-side failures (outside the -32099..-32000 server range, so
// clients classify it as non-retryable).
const rpcPermanentCode = -32800

// Handler exposes the Node over HTTP JSON-RPC (eth-style methods), so the
// exact same programmable chain/failure behavior can be exercised through
// rpc.JSONRPCClient instead of in-process calls.
//
// Failure mapping (injected via FailNext/FailAlways):
//
//	ClassRateLimited -> HTTP 429
//	ClassTimeout     -> HTTP 408
//	ClassTemporary   -> HTTP 500
//	ClassPermanent   -> JSON-RPC error code -32800
//
// A missing block is reported as a null result (standard eth behavior), which
// clients map to rpc.ErrBlockNotFound.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.serveRPC)
	return mux
}

type serverRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

type serverResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *serverError    `json:"error,omitempty"`
}

type serverError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type blockWire struct {
	Number     string        `json:"number"`
	Hash       string        `json:"hash"`
	ParentHash string        `json:"parentHash"`
	Events     []types.Event `json:"events"`
}

func (n *Node) serveRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req serverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeRPCError(w, req.ID, -32700, "parse error: "+err.Error())
		return
	}
	switch req.Method {
	case "eth_blockNumber":
		n.serveTip(w, r, req)
	case "eth_getBlockByNumber":
		n.serveBlock(w, r, req)
	default:
		writeRPCError(w, req.ID, -32601, "method not found: "+req.Method)
	}
}

func (n *Node) serveTip(w http.ResponseWriter, r *http.Request, req serverRequest) {
	tip, err := n.ChainTip(r.Context())
	if err != nil {
		writeNodeError(w, req.ID, err)
		return
	}
	writeRPCResult(w, req.ID, "0x"+strconv.FormatUint(tip, 16))
}

func (n *Node) serveBlock(w http.ResponseWriter, r *http.Request, req serverRequest) {
	if len(req.Params) < 1 {
		writeRPCError(w, req.ID, -32602, "missing block number param")
		return
	}
	var hexNum string
	if err := json.Unmarshal(req.Params[0], &hexNum); err != nil {
		writeRPCError(w, req.ID, -32602, "bad block number param")
		return
	}
	num, err := strconv.ParseUint(strings.TrimPrefix(hexNum, "0x"), 16, 64)
	if err != nil {
		writeRPCError(w, req.ID, -32602, "bad block number: "+hexNum)
		return
	}
	b, err := n.BlockByNumber(r.Context(), num)
	if err != nil {
		if errors.Is(err, rpc.ErrBlockNotFound) {
			writeRPCResult(w, req.ID, nil) // null result: no block at height
			return
		}
		writeNodeError(w, req.ID, err)
		return
	}
	events := b.Events
	if events == nil {
		events = []types.Event{}
	}
	writeRPCResult(w, req.ID, blockWire{
		Number:     "0x" + strconv.FormatUint(b.Number, 16),
		Hash:       b.Hash,
		ParentHash: b.ParentHash,
		Events:     events,
	})
}

// writeNodeError maps an injected node failure to the wire.
func writeNodeError(w http.ResponseWriter, id json.RawMessage, err error) {
	switch rpc.ClassOf(err) {
	case rpc.ClassRateLimited:
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	case rpc.ClassTimeout:
		http.Error(w, "upstream timeout", http.StatusRequestTimeout)
	case rpc.ClassTemporary:
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	default:
		writeRPCError(w, id, rpcPermanentCode, err.Error())
	}
}

func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result any) {
	writeJSON(w, serverResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	writeJSON(w, serverResponse{JSONRPC: "2.0", ID: id, Error: &serverError{Code: code, Message: msg}})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
