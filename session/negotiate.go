package session

import (
	"context"
	"fmt"
	"time"

	"github.com/h3yow/qkd-key-pool-go/link"
	"github.com/h3yow/qkd-key-pool-go/proto"
	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

// msg builds a protocol message for the current session.
func (n *Negotiation) msg(step proto.Step, seq int, qber float64, reason string) proto.Message {
	m := proto.Message{
		Version: proto.SupportedVersion,
		Session: n.cfg.SessionID,
		Step:    step,
		Seq:     seq,
		Reason:  reason,
	}
	if step == proto.StepParameter {
		m.Payload = map[string]float64{"qber": qber}
	}
	return m
}

// stepCtx derives a bounded context for one protocol step.
func (n *Negotiation) stepCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, n.cfg.StepTimeout)
}

// send delivers from->to, mapping cancellation/deadline onto KindTimeout.
func (n *Negotiation) send(ctx context.Context, from, to link.Endpoint, m proto.Message) error {
	if err := n.lk.Deliver(ctx, from, to, m); err != nil {
		if qkderr.KindOf(err) == qkderr.KindTimeout {
			return err
		}
		return err
	}
	return nil
}

// recv waits for a frame at 'at'.
func (n *Negotiation) recv(ctx context.Context, at link.Endpoint) (link.Frame, error) {
	return n.lk.Recv(ctx, at)
}

// stop records a terminal state and its classified reason.
func (n *Negotiation) stop(st State, err error) error {
	n.state = st
	n.reason = err
	n.finished = true
	n.log.Printf("session %s -> terminal state %s (%v)", n.cfg.SessionID, st, err)
	return err
}

// responder runs the peer role and returns a classified error describing the
// responder-side outcome; the initiator observes this via frames (or the lack
// thereof / an abort frame).
func (n *Negotiation) responder(ctx context.Context) error {
	// 1) wait for hello
	sctx, cancel := n.stepCtx(ctx)
	f, err := n.recv(sctx, link.EndpointB)
	cancel()
	if err != nil {
		return err
	}
	hm := f.Msg
	if hm.Step != proto.StepHello {
		return qkderr.New(qkderr.KindOutOfOrderMessage, "expected hello first")
	}
	if hm.Version != n.cfg.PeerVersion {
		// Peer negotiates a different version: reply abort explicitly.
		m := n.msg(proto.StepAbort, proto.ExpectedSeq(proto.StepAbort), 0,
			ReasonVersionIncompat)
		_ = n.send(ctx, link.EndpointB, link.EndpointA, m)
		return qkderr.New(qkderr.KindUnsupportedVersion,
			"peer protocol version incompatible")
	}

	// 2) sift; responder may be instructed to abort here.
	if n.cfg.PeerForceAbort {
		m := n.msg(proto.StepAbort, proto.ExpectedSeq(proto.StepAbort), 0,
			"peer aborted during sifting")
		if err := n.send(ctx, link.EndpointB, link.EndpointA, m); err != nil {
			return err
		}
		return qkderr.New(qkderr.KindPeerAborted, "peer aborted during sifting")
	}
	sift := n.msg(proto.StepSift, proto.ExpectedSeq(proto.StepSift), 0, "")
	if err := n.send(ctx, link.EndpointB, link.EndpointA, sift); err != nil {
		return err
	}

	// 3) parameter estimation. Initiator reports its measured QBER; responder
	// independently measures and reports its own.
	sctx, cancel = n.stepCtx(ctx)
	f, err = n.recv(sctx, link.EndpointB)
	cancel()
	if err != nil {
		return err
	}
	if f.Msg.Step == proto.StepAbort {
		return qkderr.New(qkderr.KindPeerAborted,
			"initiator aborted: "+safeReason(f.Msg.Reason))
	}
	if f.Msg.Step != proto.StepParameter {
		return qkderr.New(qkderr.KindOutOfOrderMessage, "expected parameter-estimate")
	}
	peerQBER := f.Msg.QBER()
	respQBER := n.lk.QBER()
	n.log.Printf("responder measured QBER=%.4f (initiator reported %.4f)",
		respQBER, peerQBER)
	pm := n.msg(proto.StepParameter, proto.ExpectedSeq(proto.StepParameter), respQBER, "")
	if err := n.send(ctx, link.EndpointB, link.EndpointA, pm); err != nil {
		return err
	}

	// 4) confirmation (may be replaced by an explicit abort to simulate a
	// confirmation failure that the initiator can distinguish immediately).
	if n.cfg.PeerFailConfirm {
		n.log.Printf("responder configured to fail confirmation: sending abort")
		ab := n.msg(proto.StepAbort, proto.ExpectedSeq(proto.StepAbort), 0,
			ReasonConfirmFailed)
		if err := n.send(ctx, link.EndpointB, link.EndpointA, ab); err != nil {
			return err
		}
		return qkderr.New(qkderr.KindConfirmFailed, "peer withheld confirmation")
	}
	cf := n.msg(proto.StepConfirm, proto.ExpectedSeq(proto.StepConfirm), 0, "")
	if err := n.send(ctx, link.EndpointB, link.EndpointA, cf); err != nil {
		return err
	}
	return nil
}

func safeReason(r string) string {
	if r == "" {
		return "unspecified"
	}
	return r
}

// classifyPeerAbort maps a received abort frame onto the correct terminal
// state/class, recognizing structured reasons (e.g. version incompatibility)
// so callers are not forced to parse free-form text.
func (n *Negotiation) classifyPeerAbort(m proto.Message) error {
	switch m.Reason {
	case ReasonVersionIncompat:
		return n.stop(StateAbortedProtocol, qkderr.New(
			qkderr.KindUnsupportedVersion, "peer protocol version incompatible"))
	case ReasonQBER:
		return n.stop(StateAbortedQBER, qkderr.New(
			qkderr.KindQBERTooHigh, "peer measured QBER above threshold"))
	default:
		return n.stop(StateAbortedPeer, qkderr.New(qkderr.KindPeerAborted,
			"peer aborted: "+safeReason(m.Reason)))
	}
}

// Run drives the full negotiation to a terminal state and returns the
// classified error (nil on StateEstablished). It is safe to call once.
func (n *Negotiation) Run(ctx context.Context) error {
	n.state = StateInit
	n.log.Printf("starting session %s threshold=%.4f link={loss=%.2f disturb=%.2f noise=%.2f seed=%d}",
		n.cfg.SessionID, n.cfg.QBERThreshold,
		n.cfg.Link.LossRate, n.cfg.Link.Disturbance, n.cfg.Link.DetectorNoise,
		n.cfg.Link.Seed)

	respErrCh := make(chan error, 1)
	go func() { respErrCh <- n.responder(ctx) }()

	// --- initiator side, sequential steps ---

	// hello
	n.state = StateHelloSent
	hello := n.msg(proto.StepHello, proto.ExpectedSeq(proto.StepHello), 0, "")
	if err := n.send(ctx, link.EndpointA, link.EndpointB, hello); err != nil {
		return n.failFromTransport(ctx, err, StateFailed)
	}

	// wait sift (could receive an abort / version rejection)
	n.state = StateSifting
	sctx, cancel := n.stepCtx(ctx)
	f, err := n.recv(sctx, link.EndpointA)
	cancel()
	if err != nil {
		return n.failFromTransport(ctx, err, StateAbortedTimeout)
	}
	switch f.Msg.Step {
	case proto.StepAbort:
		return n.classifyPeerAbort(f.Msg)
	case proto.StepSift:
		// proceed
	default:
		return n.stop(StateAbortedProtocol, qkderr.New(qkderr.KindOutOfOrderMessage,
			fmt.Sprintf("expected sift, got %s", f.Msg.Step)))
	}

	// parameter estimation
	n.state = StateEstimating
	measured := n.lk.QBER()
	n.log.Printf("initiator measured QBER=%.4f vs threshold=%.4f",
		measured, n.cfg.QBERThreshold)
	if measured > n.cfg.QBERThreshold {
		ab := n.msg(proto.StepAbort, proto.ExpectedSeq(proto.StepAbort), 0,
			ReasonQBER)
		_ = n.send(ctx, link.EndpointA, link.EndpointB, ab)
		return n.stop(StateAbortedQBER, qkderr.New(qkderr.KindQBERTooHigh,
			fmt.Sprintf("measured QBER %.4f exceeds threshold %.4f",
				measured, n.cfg.QBERThreshold)))
	}
	pm := n.msg(proto.StepParameter, proto.ExpectedSeq(proto.StepParameter), measured, "")
	if err := n.send(ctx, link.EndpointA, link.EndpointB, pm); err != nil {
		return n.failFromTransport(ctx, err, StateFailed)
	}

	// receive responder's parameter estimate (or abort)
	sctx, cancel = n.stepCtx(ctx)
	f, err = n.recv(sctx, link.EndpointA)
	cancel()
	if err != nil {
		return n.failFromTransport(ctx, err, StateAbortedTimeout)
	}
	if f.Msg.Step == proto.StepAbort {
		return n.stop(StateAbortedPeer, qkderr.New(qkderr.KindPeerAborted,
			"peer aborted: "+safeReason(f.Msg.Reason)))
	}
	if f.Msg.Step != proto.StepParameter {
		return n.stop(StateAbortedProtocol, qkderr.New(qkderr.KindOutOfOrderMessage,
			fmt.Sprintf("expected parameter-estimate, got %s", f.Msg.Step)))
	}
	if rq := f.Msg.QBER(); rq > n.cfg.QBERThreshold {
		return n.stop(StateAbortedQBER, qkderr.New(qkderr.KindQBERTooHigh,
			fmt.Sprintf("peer QBER %.4f exceeds threshold %.4f", rq, n.cfg.QBERThreshold)))
	}

	// confirmation
	n.state = StateConfirming
	sctx, cancel = n.stepCtx(ctx)
	f, err = n.recv(sctx, link.EndpointA)
	cancel()
	if err != nil {
		// Missing confirmation within the step window => confirmation failure.
		if qkderr.KindOf(err) == qkderr.KindTimeout {
			return n.stop(StateAbortedConfirm,
				qkderr.New(qkderr.KindConfirmFailed, "confirmation not received in time"))
		}
		return n.failFromTransport(ctx, err, StateAbortedConfirm)
	}
	switch f.Msg.Step {
	case proto.StepConfirm:
		// established below
	case proto.StepAbort:
		if f.Msg.Reason == ReasonConfirmFailed {
			return n.stop(StateAbortedConfirm,
				qkderr.New(qkderr.KindConfirmFailed, "peer failed to confirm"))
		}
		return n.stop(StateAbortedPeer, qkderr.New(qkderr.KindPeerAborted,
			"peer aborted: "+safeReason(f.Msg.Reason)))
	default:
		return n.stop(StateAbortedProtocol, qkderr.New(qkderr.KindConfirmFailed,
			"expected confirmation frame, got "+string(f.Msg.Step)))
	}

	// let the responder finish cleanly
	select {
	case rerr := <-respErrCh:
		if rerr != nil && qkderr.KindOf(rerr) == qkderr.KindConfirmFailed {
			return n.stop(StateAbortedConfirm, rerr)
		}
	case <-ctx.Done():
		return n.stop(StateAbortedTimeout,
			qkderr.Wrap(qkderr.KindTimeout, "negotiation cancelled", ctx.Err()))
	case <-time.After(100 * time.Millisecond):
	}

	n.state = StateEstablished
	n.finished = true
	n.reason = nil
	n.log.Printf("session %s established", n.cfg.SessionID)
	return nil
}

// failFromTransport maps a lower-level transport error onto a terminal state,
// translating timeouts distinctly from generic/internal failures and never
// leaking an unmapped cause.
func (n *Negotiation) failFromTransport(ctx context.Context, err error, fallback State) error {
	switch qkderr.KindOf(err) {
	case qkderr.KindTimeout:
		return n.stop(StateAbortedTimeout, err)
	case qkderr.KindDropped:
		// Persistent frame loss aborts confirmation/negotiation unambiguously.
		return n.stop(StateAbortedConfirm, err)
	default:
		return n.stop(fallback, qkderr.Wrap(qkderr.KindInternal, "transport failure", err))
	}
}
