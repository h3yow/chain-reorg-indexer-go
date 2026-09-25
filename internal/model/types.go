package model

// Block is a minimal block view returned by a JSON-RPC node.
type Block struct {
	Number     uint64
	Hash       string
	ParentHash string
	Events     []Event
}

// Event is a chain event (e.g. decoded log) belonging to a block.
type Event struct {
	Index       int
	BlockNumber uint64
	BlockHash   string
	Kind        string
	Payload     string
}

// ChainHead is the node's view of the current chain tip.
type ChainHead struct {
	Number uint64
	Hash   string
}

// Key returns a stable identity for dedup purposes.
func (b Block) Key() string { return b.Hash }
