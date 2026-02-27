package connection

import (
	"errors"
	"fmt"
)

// ErrorCode classifies SDK/runtime failures for retry and diagnostics policies.
type ErrorCode string

const (
	ErrorInvalidArgument  ErrorCode = "invalid_argument"
	ErrorConnectionClosed ErrorCode = "connection_closed"
	ErrorEncodeFailed     ErrorCode = "encode_failed"
	ErrorSendFailed       ErrorCode = "send_failed"
	ErrorUnexpectedKind   ErrorCode = "unexpected_message_kind"
)

// Error is the canonical error wrapper for SDK operations.
type Error struct {
	Code ErrorCode
	Op   string
	Err  error
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Op == "" {
		return fmt.Sprintf("%s: %v", e.Code, e.Err)
	}
	return fmt.Sprintf("%s (%s): %v", e.Code, e.Op, e.Err)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func wrapError(code ErrorCode, op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Code: code, Op: op, Err: err}
}

func newInvalidArgument(op string, msg string) error {
	return &Error{
		Code: ErrorInvalidArgument,
		Op:   op,
		Err:  errors.New(msg),
	}
}

func newUnexpectedKind(op string, got, want string) error {
	return &Error{
		Code: ErrorUnexpectedKind,
		Op:   op,
		Err:  fmt.Errorf("unexpected result kind: got %q want %q", got, want),
	}
}

// IsCode reports whether err (or any wrapped error) is an SDK Error with the given code.
func IsCode(err error, code ErrorCode) bool {
	var sdkErr *Error
	return errors.As(err, &sdkErr) && sdkErr.Code == code
}
