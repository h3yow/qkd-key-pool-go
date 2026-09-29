package session

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/qkd-key-pool-go/link"
	"github.com/h3yow/qkd-key-pool-go/proto"
	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

// testLogger routes session logs into the test output so key inputs, state
// transitions and decisions are visible with `go test -v`.
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("[session] "+format, args...)
}

func runNeg(t *testing.T, cfg Config) (*Negotiation, error) {
	t.Helper()
	n := New(cfg, testLogger{t})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := n.Run(ctx)
	t.Logf("final state=%s err=%v", n.State(), err)
	return n, err
}

func TestNegotiation_HappyPath(t *testing.T) {
	n, err := runNeg(t, Config{
		SessionID:     "happy",
		QBERThreshold: 0.11,
		Link: link.Config{
			BaseQBER: 0.02, Disturbance: 0.01, DetectorNoise: 0.02,
			LossRate: 0.0, Seed: 1, MaxAttempts: 3,
		},
	})
	if err != nil {
		t.Fatalf("expected establishment, got %v (kind=%s)", err, qkderr.KindOf(err))
	}
	if n.State() != StateEstablished {
		t.Fatalf("state = %s, want established", n.State())
	}
}

func TestNegotiation_HighQBER_AbortsDistinguishably(t *testing.T) {
	// disturbance dominates the QBER formula -> reliably over threshold.
	n, err := runNeg(t, Config{
		SessionID:     "high-qber",
		QBERThreshold: 0.11,
		Link: link.Config{
			BaseQBER: 0.02, Disturbance: 0.25, DetectorNoise: 0.02,
			Seed: 7,
		},
	})
	if err == nil {
		t.Fatal("expected abort due to high QBER, got nil")
	}
	if got := qkderr.KindOf(err); got != qkderr.KindQBERTooHigh {
		t.Fatalf("kind = %s, want %s", got, qkderr.KindQBERTooHigh)
	}
	if n.State() != StateAbortedQBER {
		t.Fatalf("state = %s, want aborted-qber", n.State())
	}
}

func TestNegotiation_PeerAbort(t *testing.T) {
	n, err := runNeg(t, Config{
		SessionID:      "peer-abort",
		QBERThreshold:  0.5,
		PeerForceAbort: true,
		Link:           link.Config{BaseQBER: 0.01, Seed: 3},
	})
	if qkderr.KindOf(err) != qkderr.KindPeerAborted {
		t.Fatalf("kind = %s, want peer-aborted (err=%v)", qkderr.KindOf(err), err)
	}
	if n.State() != StateAbortedPeer {
		t.Fatalf("state = %s, want aborted-peer", n.State())
	}
}

func TestNegotiation_ConfirmFailure(t *testing.T) {
	n, err := runNeg(t, Config{
		SessionID:       "confirm-fail",
		QBERThreshold:   0.5,
		PeerFailConfirm: true,
		Link:            link.Config{BaseQBER: 0.01, Seed: 3},
	})
	if qkderr.KindOf(err) != qkderr.KindConfirmFailed {
		t.Fatalf("kind = %s, want confirm-failed (err=%v)", qkderr.KindOf(err), err)
	}
	if n.State() != StateAbortedConfirm {
		t.Fatalf("state = %s, want aborted-confirm", n.State())
	}
}

func TestNegotiation_VersionIncompatible(t *testing.T) {
	n, err := runNeg(t, Config{
		SessionID:     "version",
		QBERThreshold: 0.5,
		PeerVersion:   "qkd-sim/0.9",
		Link:          link.Config{BaseQBER: 0.01, Seed: 3},
	})
	if qkderr.KindOf(err) != qkderr.KindUnsupportedVersion {
		t.Fatalf("kind = %s, want unsupported-version (err=%v)", qkderr.KindOf(err), err)
	}
	if n.State() != StateAbortedProtocol {
		t.Fatalf("state = %s, want aborted-protocol via version reject", n.State())
	}
}

func TestNegotiation_HeavyLoss_ConfirmAbort(t *testing.T) {
	// Loss 1.0 exhausts every delivery attempt; the very first frame fails.
	_, err := runNeg(t, Config{
		SessionID:     "loss",
		QBERThreshold: 0.5,
		StepTimeout:   500 * time.Millisecond,
		Link: link.Config{
			BaseQBER: 0.01, LossRate: 1.0, MaxAttempts: 2, Seed: 5,
		},
	})
	k := qkderr.KindOf(err)
	if k != qkderr.KindDropped && k != qkderr.KindTimeout {
		t.Fatalf("kind = %s, want dropped or timeout (err=%v)", k, err)
	}
}

// ---- ingress message classification ----

func newIdleNeg(t *testing.T) *Negotiation {
	return New(Config{SessionID: "ing"}, testLogger{t})
}

func msg(step proto.Step, seq int, session string) *proto.Message {
	return &proto.Message{
		Version: proto.SupportedVersion, Session: session, Step: step, Seq: seq,
	}
}

func TestHandleMessage_OrderedThenDuplicate(t *testing.T) {
	n := newIdleNeg(t)
	first := msg(proto.StepHello, 1, "ing")
	if err := n.HandleMessage(first); err != nil {
		t.Fatalf("first hello should be accepted, got %v", err)
	}
	err := n.HandleMessage(msg(proto.StepHello, 1, "ing"))
	if qkderr.KindOf(err) != qkderr.KindDuplicateMessage {
		t.Fatalf("repeated hello: kind = %s, want duplicate-message", qkderr.KindOf(err))
	}
}

func TestHandleMessage_OutOfOrder(t *testing.T) {
	n := newIdleNeg(t)
	// skip hello (seq1), jump straight to sift (seq2).
	err := n.HandleMessage(msg(proto.StepSift, 2, "ing"))
	if qkderr.KindOf(err) != qkderr.KindOutOfOrderMessage {
		t.Fatalf("kind = %s, want out-of-order-message (err=%v)", qkderr.KindOf(err), err)
	}
}

func TestHandleMessage_StaleSessionAndTerminal(t *testing.T) {
	n := newIdleNeg(t)
	// message for an unknown/closed session
	err := n.HandleMessage(msg(proto.StepHello, 1, "other-session"))
	if qkderr.KindOf(err) != qkderr.KindStaleMessage {
		t.Fatalf("foreign session: kind = %s, want stale-message", qkderr.KindOf(err))
	}
	// after termination, a late message is stale (never silently ignored)
	n.finished = true
	n.state = StateEstablished
	err = n.HandleMessage(msg(proto.StepHello, 1, "ing"))
	if qkderr.KindOf(err) != qkderr.KindStaleMessage {
		t.Fatalf("late-after-terminal: kind = %s, want stale-message", qkderr.KindOf(err))
	}
}

func TestHandleMessage_UnsupportedVersion(t *testing.T) {
	n := newIdleNeg(t)
	m := msg(proto.StepHello, 1, "ing")
	m.Version = "qkd-sim/2"
	err := n.HandleMessage(m)
	if qkderr.KindOf(err) != qkderr.KindUnsupportedVersion {
		t.Fatalf("kind = %s, want unsupported-version (err=%v)", qkderr.KindOf(err), err)
	}
}

func TestHandleMessage_Malformed(t *testing.T) {
	n := newIdleNeg(t)
	cases := map[string]*proto.Message{
		"empty version":     {Session: "ing", Step: proto.StepHello, Seq: 1},
		"bad step":          {Version: proto.SupportedVersion, Session: "ing", Step: "bogus", Seq: 1},
		"zero seq":          {Version: proto.SupportedVersion, Session: "ing", Step: proto.StepHello, Seq: 0},
		"step/seq mismatch": {Version: proto.SupportedVersion, Session: "ing", Step: proto.StepHello, Seq: 2},
	}
	for name, m := range cases {
		err := n.HandleMessage(m)
		if qkderr.KindOf(err) != qkderr.KindMalformed &&
			qkderr.KindOf(err) != qkderr.KindUnsupportedVersion {
			t.Fatalf("%s: kind = %s, want malformed/unsupported", name, qkderr.KindOf(err))
		}
		t.Logf("malformed case %-16s -> %s (%v)", name, qkderr.KindOf(err), err)
	}
}
