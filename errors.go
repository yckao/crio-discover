package discover

import "errors"

// Public sentinel errors for invalid API usage and testable caller handling.
var (
	ErrNilContext           = errors.New("context is nil")
	ErrNilHandler           = errors.New("handler is nil")
	ErrMissingRuntimeClient = errors.New("runtime client is nil")
)
