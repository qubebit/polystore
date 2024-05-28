package types

import "errors"

var (
	ErrNotSupportByBackend = errors.New("operation not supported by backend")
)
