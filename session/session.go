// Package session implements the QKD session negotiation state machine over
// the local simulated link: hello -> sift -> parameter estimation (QBER gate)
// -> confirmation -> established, with explicit, distinguishable abort states.
//
// A negotiation is fully local: one goroutine plays the initiator and one plays
// the responder, exchanging frames through a *link.Link. Either side can be
// configured to misbehave (abort, fail confirmation, speak another version) so
// that every terminal path is reproducible.
package session

import (
	"fmt"
	"time"

	"github.com/h3yow/qkd-key-pool-go/link"
	"github.com/h3yow/qkd-key-pool-go/proto"
	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

// State is an explicit session state. A stopped session always rests in a
// terminal state, so callers can tell exactly where it got stuck.
type State int

const (
	StateInit State = iota
	StateHelloSent
	StateSifting
	StateEstimating
	StateConfirming
	StateEstablished
	StateAbortedQBER
	StateAbortedConfirm
	StateAbortedPeer
	StateAbortedTimeout
	StateAbortedProtocol
	StateFailed
)

var stateNames = [...]string{
	"init", "hello-sent", "sifting", "estimating", "confirming", "established",
	"aborted-qber", "aborted-confirm", "aborted-peer", "aborted-timeout",
	"aborted-protocol", "failed",
}

func (s State) String() string {
	if int(s) < 0 || int(s) >= len(stateNames) {
		return "unknown"
	}
	return stateNames[s]
}

// Terminal reports whether no further state transitions are possible.
func (s State) Terminal() bool {
	switch s {
	case StateEstablished, StateAbortedQBER, StateAbortedConfirm,
		StateAbortedPeer, StateAbortedTimeout, StateAbortedProtocol, StateFailed:
		return true
	default:
		return false
	}
}

// Config configures a negotiation run.
type Config struct {
	Link          link.Config
	QBERThreshold float64 // abort when measured QBER exceeds this
	StepTimeout   time.Duration
	SessionID     string
	// PeerFaults configure responder misbehaviour for reproducible failures.
	PeerForceAbort  bool // responder sends an abort during sifting
	PeerFailConfirm bool // responder never sends a confirm
	PeerVersion     proto.Version
	// Reorder/duplicate handling is exercised directly via HandleMessage.
}

// Logger receives structured, human-readable progress lines. Tests pass a
// logger backed by t.Logf so key inputs, transitions and decisions are visible.
type Logger interface {
	Printf(format string, args ...any)
}

// Negotiation models one local two-party negotiation.
type Negotiation struct {
	cfg    Config
	lk     *link.Link
	log    Logger
	state  State
	reason error

	// expectedSeq is the next ingress step the receiver is willing to accept.
	expectedSeq int
	finished    bool
}

// New creates a negotiation. A nil logger is replaced with a no-op.
func New(cfg Config, log Logger) *Negotiation {
	if cfg.QBERThreshold <= 0 {
		cfg.QBERThreshold = 0.11
	}
	if cfg.StepTimeout <= 0 {
		cfg.StepTimeout = 2 * time.Second
	}
	if cfg.SessionID == "" {
		cfg.SessionID = "sess-1"
	}
	if cfg.PeerVersion == "" {
		cfg.PeerVersion = proto.SupportedVersion
	}
	if log == nil {
		log = nopLogger{}
	}
	return &Negotiation{
		cfg:         cfg,
		lk:          link.New(cfg.Link),
		log:         log,
		state:       StateInit,
		expectedSeq: proto.ExpectedSeq(proto.StepHello),
	}
}

// Link exposes the underlying link for callers/tests that want to inject
// frames or inspect the queues.
func (n *Negotiation) Link() *link.Link { return n.lk }

// State returns the current state.
func (n *Negotiation) State() State { return n.state }

// Err returns the classified reason the negotiation stopped (nil while live
// or after a clean establishment).
func (n *Negotiation) Err() error { return n.reason }

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// HandleMessage applies ingress classification to one message: structural /
// version validation, then duplicate, out-of-order and stale detection. It is
// the single place where "must not silently ignore" is enforced; every refusal
// comes back as a distinguishable qkderr.Kind. It does not advance the normal
// protocol beyond acknowledging a correctly-ordered step.
func (n *Negotiation) HandleMessage(m *proto.Message) error {
	if err := m.Validate(); err != nil {
		// version vs. malformed already distinguished by Validate.
		n.log.Printf("ingress reject: %v", err)
		return err
	}
	if m.Session != n.cfg.SessionID {
		return qkderr.New(qkderr.KindStaleMessage,
			fmt.Sprintf("message for unknown/closed session %q", m.Session))
	}
	if n.finished || n.state.Terminal() {
		return qkderr.New(qkderr.KindStaleMessage,
			fmt.Sprintf("session already %s; late %s message rejected", n.state, m.Step))
	}

	want := proto.ExpectedSeq(m.Step)
	if m.Seq != want {
		return qkderr.New(qkderr.KindMalformed,
			fmt.Sprintf("step %s carries seq=%d but canonical seq is %d",
				m.Step, m.Seq, want))
	}
	switch {
	case want < n.expectedSeq:
		n.log.Printf("ingress duplicate: step=%s seq=%d expected>=%d",
			m.Step, want, n.expectedSeq)
		return qkderr.New(qkderr.KindDuplicateMessage,
			fmt.Sprintf("duplicate %s message (seq=%d)", m.Step, want))
	case want > n.expectedSeq:
		n.log.Printf("ingress out-of-order: step=%s seq=%d expected=%d",
			m.Step, want, n.expectedSeq)
		return qkderr.New(qkderr.KindOutOfOrderMessage,
			fmt.Sprintf("out-of-order %s message (seq=%d, want=%d)",
				m.Step, want, n.expectedSeq))
	default:
		n.expectedSeq++
		n.log.Printf("ingress accepted: step=%s seq=%d", m.Step, want)
		return nil
	}
}

// Standard abort reasons exchanged on the classical channel. They are safe to
// surface to callers and let the receiver classify an abort precisely.
const (
	ReasonConfirmFailed   = "confirm-failed"
	ReasonQBER            = "qber-too-high"
	ReasonVersionIncompat = "version-incompatible"
)
