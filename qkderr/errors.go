// Package qkderr defines classified, safe errors for the local QKD simulation.
//
// Internal/implementation errors are never returned verbatim to callers;
// they are wrapped and surfaced as a stable ErrorKind instead.
package qkderr

import "errors"

// Kind is the stable, distinguishable failure category.
type Kind int

const (
	KindUnknown Kind = iota
	// KindQBERTooHigh: measured quantum bit error rate exceeds the threshold.
	KindQBERTooHigh
	// KindConfirmFailed: classical confirmation step did not complete.
	KindConfirmFailed
	// KindPeerAborted: the peer deliberately aborted the session.
	KindPeerAborted
	// KindTimeout: a step or lease timed out.
	KindTimeout
	// KindUnsupportedVersion: protocol version mismatch / incompatibility.
	KindUnsupportedVersion
	// KindDuplicateMessage: message for a step already processed.
	KindDuplicateMessage
	// KindOutOfOrderMessage: message arrived before its predecessor.
	KindOutOfOrderMessage
	// KindStaleMessage: message belongs to an expired/closed session.
	KindStaleMessage
	// KindKeyUnavailable: no key can satisfy the lease request.
	KindKeyUnavailable
	// KindKeyConsumed: the key was already confirmed-consumed.
	KindKeyConsumed
	// KindKeyExpired: the key passed its expiry time.
	KindKeyExpired
	// KindLeaseNotHeld: caller does not hold a valid lease for the operation.
	KindLeaseNotHeld
	// KindInvalidState: operation not allowed in the current session/key state.
	KindInvalidState
	// KindDropped: simulated frame loss exhausted retries.
	KindDropped
	// KindMalformed: message failed structural validation.
	KindMalformed
	// KindInternal: internal failure. Detail is hidden from external Error().
	KindInternal
)

// Error is the classified error type. Safe for callers; internal causes are
// retained via Unwrap for logging but omitted from Error() unless exposed.
type Error struct {
	Kind Kind
	// Msg is a short, safe, parameter-free-ish description.
	Msg string
	// cause is internal and never rendered by Error().
	cause error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg != "" {
		return e.Kind.String() + ": " + e.Msg
	}
	return e.Kind.String()
}

func (e *Error) Unwrap() error { return e.cause }

// New builds an error of the given kind with a safe message.
func New(kind Kind, msg string) error {
	return &Error{Kind: kind, Msg: msg}
}

// Wrap hides an internal cause behind a classified error.
func Wrap(kind Kind, msg string, cause error) error {
	return &Error{Kind: kind, Msg: msg, cause: cause}
}

// Internal reports an internal failure without leaking the cause text.
func Internal(cause error) error {
	return &Error{Kind: KindInternal, Msg: "internal error", cause: cause}
}

// KindOf extracts the Kind of an error, KindUnknown if not classified.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindUnknown
}

func (k Kind) String() string {
	switch k {
	case KindQBERTooHigh:
		return "qber-too-high"
	case KindConfirmFailed:
		return "confirm-failed"
	case KindPeerAborted:
		return "peer-aborted"
	case KindTimeout:
		return "timeout"
	case KindUnsupportedVersion:
		return "unsupported-version"
	case KindDuplicateMessage:
		return "duplicate-message"
	case KindOutOfOrderMessage:
		return "out-of-order-message"
	case KindStaleMessage:
		return "stale-message"
	case KindKeyUnavailable:
		return "key-unavailable"
	case KindKeyConsumed:
		return "key-consumed"
	case KindKeyExpired:
		return "key-expired"
	case KindLeaseNotHeld:
		return "lease-not-held"
	case KindInvalidState:
		return "invalid-state"
	case KindDropped:
		return "dropped"
	case KindMalformed:
		return "malformed"
	case KindInternal:
		return "internal"
	default:
		return "unknown"
	}
}

// ErrNotImplemented is a temporary sentinel used by skeleton stubs.
var ErrNotImplemented = errors.New("not implemented")
