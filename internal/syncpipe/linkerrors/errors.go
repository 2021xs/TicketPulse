// Package linkerrors classifies projection failures for retry and isolation.
package linkerrors

import (
	"context"
	stderrors "errors"
)

type Kind string

const (
	KindTransient Kind = "transient"
	KindRetryable Kind = "retryable"
	KindPermanent Kind = "permanent"
	KindSystem    Kind = "system"
)

type Error struct {
	Kind Kind
	Code string
	Err  error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return string(e.Kind) + ": " + e.Err.Error()
	}
	return string(e.Kind) + ": " + e.Code + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func New(kind Kind, code string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: kind, Code: code, Err: err}
}

func Transient(code string, err error) error { return New(KindTransient, code, err) }
func Retryable(code string, err error) error { return New(KindRetryable, code, err) }
func Permanent(code string, err error) error { return New(KindPermanent, code, err) }
func System(code string, err error) error    { return New(KindSystem, code, err) }

func KindOf(err error) Kind {
	if err == nil {
		return ""
	}
	// An explicit *Error wrapper takes precedence over the underlying context
	// error: callers that wrap context.Canceled/DeadlineExceeded with a kind
	// (e.g. Retryable for a per-batch processing timeout) intend that kind to
	// drive classification. Unwrapped context errors still default to System.
	var e *Error
	if stderrors.As(err, &e) {
		return e.Kind
	}
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return KindSystem
	}
	return KindPermanent
}

func IsTransient(err error) bool { return KindOf(err) == KindTransient }
func IsRetryable(err error) bool { return KindOf(err) == KindRetryable }
func IsPermanent(err error) bool { return KindOf(err) == KindPermanent }
func IsSystem(err error) bool    { return KindOf(err) == KindSystem }
