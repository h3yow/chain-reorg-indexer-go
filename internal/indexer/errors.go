package indexer

import (
	"errors"
	"fmt"
)

// Distinguishable indexer failure reasons.
var (
	// ErrHeightJump: next block did not extend head by exactly one.
	ErrHeightJump = errors.New("indexer: height jump, missing intermediate block")
	// ErrMissingData: the node explicitly had no block for a needed height.
	ErrMissingData = errors.New("indexer: missing block data on node")
	// ErrParentMismatch: a block's parent hash did not link to our chain;
	// this triggers reorg handling.
	ErrParentMismatch = errors.New("indexer: parent hash mismatch (chain reorganization)")
	// ErrFinalityReorg: the node reorged deeper than the confirmation depth;
	// finalized data would have to be discarded.
	ErrFinalityReorg = errors.New("indexer: reorganization would revert finalized blocks")
	// ErrReorgNotConverging: reorg resolution did not settle within the bound.
	ErrReorgNotConverging = errors.New("indexer: chain did not converge after bounded reorg attempts")
)

// ParentMismatchError carries the details of a broken parent link.
type ParentMismatchError struct {
	Height   uint64
	Got      string
	Expected string
}

func (e *ParentMismatchError) Error() string {
	return fmt.Sprintf("%v at height %d: parent=%s expected=%s",
		ErrParentMismatch, e.Height, e.Got, e.Expected)
}

func (e *ParentMismatchError) Unwrap() error { return ErrParentMismatch }
