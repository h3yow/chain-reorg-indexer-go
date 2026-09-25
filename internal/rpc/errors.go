package rpc

import (
	"errors"
	"fmt"
)

// ErrorClass categorizes node failures so callers can react differently.
type ErrorClass int

const (
	ClassUnknown ErrorClass = iota
	ClassTimeout
	ClassTemporary
	ClassRateLimited
	ClassPermanent
)

// String returns the human-readable name of the error class.
func (c ErrorClass) String() string {
	switch c {
	case ClassTimeout:
		return "timeout"
	case ClassTemporary:
		return "temporary"
	case ClassRateLimited:
		return "rate_limited"
	case ClassPermanent:
		return "permanent"
	default:
		return "unknown"
	}
}

// Error is a classified node error.
type Error struct {
	Class ErrorClass
	Msg   string
}

func (e *Error) Error() string {
	return fmt.Sprintf("rpc error (%s): %s", e.Class, e.Msg)
}

// NewError constructs a classified rpc error.
func NewError(class ErrorClass, format string, args ...any) error {
	return &Error{Class: class, Msg: fmt.Sprintf(format, args...)}
}

// classified is implemented by custom errors that carry an ErrorClass.
type classified interface {
	Class() ErrorClass
}

// ClassOf extracts the ErrorClass from an error, defaulting to ClassUnknown.
func ClassOf(err error) ErrorClass {
	if e, ok := err.(*Error); ok {
		return e.Class
	}
	if c, ok := err.(classified); ok {
		return c.Class()
	}
	return ClassUnknown
}

// IsRetryable reports whether an error should be retried.
func IsRetryable(err error) bool {
	switch ClassOf(err) {
	case ClassTimeout, ClassTemporary, ClassRateLimited:
		return true
	default:
		return false
	}
}

// ErrBlockNotFound means the node explicitly had no block at a height.
var ErrBlockNotFound = errors.New("rpc: block not found")

// BlockNotFoundError is a permanent, non-retryable missing-data error.
type BlockNotFoundError struct {
	Height uint64
}

func (e *BlockNotFoundError) Error() string {
	return fmt.Sprintf("%v at height %d", ErrBlockNotFound, e.Height)
}

func (e *BlockNotFoundError) Is(target error) bool { return target == ErrBlockNotFound }

// Class returns rpc.ClassPermanent so the retry layer treats it as non-retryable.
func (e *BlockNotFoundError) Class() ErrorClass { return ClassPermanent }
