package fakenode

import (
	"net/http/httptest"
	"sync"

	"github.com/h3yow/chain-reorg-indexer-go/internal/model"
)

// FailureMode controls how the fake node responds, for test scripting.
type FailureMode string

const (
	FailNone      FailureMode = ""
	FailTimeout   FailureMode = "timeout"
	FailTemporary FailureMode = "temporary"
	FailRateLimit FailureMode = "rate_limit"
	FailNotFound  FailureMode = "not_found"
)

// blockWire / eventWire is the node's JSON shape.
type blockWire struct {
	Number     uint64       `json:"number"`
	Hash       string       `json:"hash"`
	ParentHash string       `json:"parentHash"`
	Events     []*eventWire `json:"events"`
}

type eventWire struct {
	Index   int    `json:"index"`
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
}

type callRecord struct {
	Method string `json:"method"`
	Height uint64 `json:"height"`
}

type failRule struct {
	mode  FailureMode
	times int
}

// Node is a programmable in-process JSON-RPC node over HTTP.
//
// Blocks are stored in an immutable pool; "canonical chain" is a
// height -> hash mapping, so SetReorg simply replaces the tail mapping.
// eth_getBlockByNumber only ever returns the canonical block at a height.
type Node struct {
	mu        sync.Mutex
	pool      map[string]*model.Block // hash -> block
	canonical map[uint64]string       // height -> hash
	failures  map[string]*failRule    // method -> rule
	latencyMS int
	calls     []Call

	srv *httptest.Server
}

// New starts a fake node serving the given canonical chain.
// The slice must be ordered by height and connected by parent hashes.
func New(chain []*model.Block) *Node {
	n := &Node{
		pool:      make(map[string]*model.Block),
		canonical: make(map[uint64]string),
		failures:  make(map[string]*failRule),
	}
	for _, b := range chain {
		n.pool[b.Hash] = b
		n.canonical[b.Number] = b.Hash
	}
	n.srv = httptest.NewServer(httpHandler(n))
	return n
}

// URL returns the HTTP endpoint.
func (n *Node) URL() string { return n.srv.URL }

// Close stops the node.
func (n *Node) Close() { n.srv.Close() }

// Call records one RPC invocation.
type Call = callRecord

// FailureSkipHeight is a scripted mode (used with SetFailure): the node
// answers eth_getBlockByNumber for the scheduled number of calls with the
// NEXT height's block instead, simulating a node that skips/serves wrong
// data at a height.
const FailureSkipHeight = "__skip_height__"
