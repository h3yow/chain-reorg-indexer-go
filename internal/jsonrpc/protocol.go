// Package jsonrpc implements a minimal JSON-RPC 2.0 over HTTP transport for
// the chain node protocol: the same rpc.Client interface is exposed over the
// network, and any rpc.Client (e.g. fakenode.Node) can be served as a node.
//
// Methods:
//   - "chain_tip"           params: []            result: hex-encoded uint64
//   - "chain_getBlockByNumber" params: ["0xN"]    result: block object or null
//
// Error codes (JSON-RPC 2.0 reserved + server-specific -320xx range):
package jsonrpc

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/h3yow/chain-reorg-indexer-go/internal/types"
)

// JSON-RPC 2.0 error codes used by this protocol.
const (
	CodeParseError     = -32700 // malformed JSON
	CodeInvalidRequest = -32600 // not a valid JSON-RPC request
	CodeMethodNotFound = -32601 // unknown method
	CodeInvalidParams  = -32602 // bad params
	CodeInternal       = -32603 // server-internal error

	CodeServerError   = -32000 // generic server-side (temporary) failure
	CodeBlockNotFound = -32001 // no block at the requested height
	CodeTimeout       = -32004 // server-side timeout
	CodeRateLimited   = -32005 // too many requests
)

// Request is a JSON-RPC 2.0 request.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response is a JSON-RPC 2.0 response. Exactly one of Result/Error is set.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *ErrorObject    `json:"error,omitempty"`
}

// ErrorObject is the JSON-RPC 2.0 error member.
type ErrorObject struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Method names served by this package.
const (
	MethodChainTip      = "chain_tip"
	MethodGetBlockByNum = "chain_getBlockByNumber"
)

// blockDTO is the wire form of types.Block.
type blockDTO struct {
	Number     uint64     `json:"number"`
	Hash       string     `json:"hash"`
	ParentHash string     `json:"parentHash"`
	Events     []eventDTO `json:"events"`
}

// eventDTO is the wire form of types.Event.
type eventDTO struct {
	Index      int    `json:"index"`
	SourceHash string `json:"sourceHash"`
	Kind       string `json:"kind"`
	Payload    string `json:"payload"`
}

// blockToDTO converts a block to its wire form.
func blockToDTO(b *types.Block) blockDTO {
	d := blockDTO{
		Number:     b.Number,
		Hash:       b.Hash,
		ParentHash: b.ParentHash,
		Events:     make([]eventDTO, len(b.Events)),
	}
	for i, ev := range b.Events {
		d.Events[i] = eventDTO{
			Index:      ev.Index,
			SourceHash: ev.SourceHash,
			Kind:       ev.Kind,
			Payload:    ev.Payload,
		}
	}
	return d
}

// blockFromDTO converts a wire block back to the domain type.
func blockFromDTO(d blockDTO) *types.Block {
	b := &types.Block{
		Number:     d.Number,
		Hash:       d.Hash,
		ParentHash: d.ParentHash,
		Events:     make([]types.Event, len(d.Events)),
	}
	for i, ev := range d.Events {
		b.Events[i] = types.Event{
			Index:      ev.Index,
			SourceHash: ev.SourceHash,
			Kind:       ev.Kind,
			Payload:    ev.Payload,
		}
	}
	return b
}

// encodeUint encodes a uint64 as a 0x-prefixed hex quantity string.
func encodeUint(v uint64) string { return "0x" + strconv.FormatUint(v, 16) }

// decodeUint parses a 0x-prefixed hex (or plain decimal) quantity string.
func decodeUint(s string) (uint64, error) {
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		return strconv.ParseUint(s[2:], 16, 64)
	}
	return strconv.ParseUint(s, 10, 64)
}

// mustJSON marshals v and panics on error (only used with known-safe types).
func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("jsonrpc: marshal %T: %v", v, err))
	}
	return raw
}
