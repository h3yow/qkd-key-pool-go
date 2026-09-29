// Package proto defines the negotiation wire messages and protocol versioning
// for the local QKD session simulation.
package proto

import (
	"fmt"

	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

// Version is the protocol version carried by every message.
type Version string

const (
	V1 Version = "qkd-sim/1"
	// SupportedVersion is the only version this build speaks.
	SupportedVersion Version = V1
)

// versions this build will accept (currently exactly V1).
var supported = map[Version]bool{V1: true}

// Step identifies the negotiation phase a message belongs to.
type Step string

const (
	StepHello     Step = "hello"
	StepSift      Step = "sift"
	StepParameter Step = "parameter-estimate" // carries measured QBER
	StepConfirm   Step = "confirm"
	StepAbort     Step = "abort"
)

// Seq numbers per step so duplicates and reordering are detectable.
var stepSeq = map[Step]int{
	StepHello:     1,
	StepSift:      2,
	StepParameter: 3,
	StepConfirm:   4,
	StepAbort:     5,
}

// ExpectedSeq returns the canonical sequence number for a step.
func ExpectedSeq(step Step) int { return stepSeq[step] }

// Message is one classical-channel frame of the negotiation protocol.
type Message struct {
	Version Version
	Session string
	Step    Step
	// Seq is the per-session step number; compare with ExpectedSeq(Step).
	Seq int
	// Payload holds step-specific data (e.g. measured QBER). Safe to log.
	Payload map[string]float64
	// Reason is set only on StepAbort; safe for the peer/caller to see.
	Reason string
}

// QBER is a convenience accessor for the measured error rate payload.
func (m *Message) QBER() float64 {
	if m.Payload == nil {
		return 0
	}
	return m.Payload["qber"]
}

// Validate performs structural and version checks. It does not decide
// ordering/duplication (that is the session state machine's job).
// Rejections carry a distinguishable Kind rather than a raw internals error.
func (m *Message) Validate() error {
	if m == nil {
		return qkderr.New(qkderr.KindMalformed, "nil message")
	}
	if m.Version == "" {
		return qkderr.New(qkderr.KindMalformed, "missing protocol version")
	}
	if !supported[m.Version] {
		return qkderr.New(
			qkderr.KindUnsupportedVersion,
			fmt.Sprintf("version %q not supported (want %q)", m.Version, SupportedVersion),
		)
	}
	if m.Session == "" {
		return qkderr.New(qkderr.KindMalformed, "missing session id")
	}
	if _, ok := stepSeq[m.Step]; !ok {
		return qkderr.New(qkderr.KindMalformed, fmt.Sprintf("unknown step %q", m.Step))
	}
	if m.Seq <= 0 {
		return qkderr.New(qkderr.KindMalformed, "non-positive sequence number")
	}
	return nil
}

// Reject describes why a message was refused, with a distinguishable Kind.
type Reject struct {
	Kind    qkderr.Kind
	Detail  string
	Session string
	Step    Step
	Seq     int
}

func (r Reject) Error() string {
	return fmt.Sprintf("%s: %s (session=%s step=%s seq=%d)",
		r.Kind, r.Detail, r.Session, r.Step, r.Seq)
}

// AsError converts the structured reject into a classified error.
func (r Reject) AsError() error { return qkderr.New(r.Kind, r.Error()) }

// IsVersionError reports whether the rejection is a version incompatibility.
func (r Reject) IsVersionError() bool { return r.Kind == qkderr.KindUnsupportedVersion }
