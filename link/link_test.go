package link

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/qkd-key-pool-go/proto"
	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

func hello() proto.Message {
	return proto.Message{
		Version: proto.SupportedVersion, Session: "s", Step: proto.StepHello, Seq: 1,
	}
}

func TestQBER_DeterministicBySeed(t *testing.T) {
	a := New(Config{BaseQBER: 0.02, Disturbance: 0.1, DetectorNoise: 0.05, Seed: 42})
	b := New(Config{BaseQBER: 0.02, Disturbance: 0.1, DetectorNoise: 0.05, Seed: 42})
	var sa, sb []float64
	for i := 0; i < 5; i++ {
		sa = append(sa, a.QBER())
		sb = append(sb, b.QBER())
	}
	for i := range sa {
		if sa[i] != sb[i] {
			t.Fatalf("QBER not reproducible: run %d %.5f != %.5f", i, sa[i], sb[i])
		}
	}
	t.Logf("reproducible QBER samples=%v", sa)
}

func TestQBER_DisturbanceDrivesAbort(t *testing.T) {
	low := New(Config{BaseQBER: 0.01, Seed: 1}).QBER()
	high := New(Config{BaseQBER: 0.01, Disturbance: 0.3, Seed: 1}).QBER()
	t.Logf("qber low=%.4f high=%.4f", low, high)
	if !(high > low) || high <= 0.3 {
		t.Fatalf("disturbance must inflate QBER above threshold: low=%.4f high=%.4f", low, high)
	}
}

func TestDeliver_ZeroLoss(t *testing.T) {
	l := New(Config{LossRate: 0, MaxAttempts: 2, Seed: 1})
	ctx := context.Background()
	if err := l.Deliver(ctx, EndpointA, EndpointB, hello()); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	rctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	f, err := l.Recv(rctx, EndpointB)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if f.From != EndpointA || f.Msg.Session != "s" {
		t.Fatalf("unexpected frame %+v", f)
	}
}

func TestDeliver_TotalLoss_ClassifiedDropped(t *testing.T) {
	l := New(Config{LossRate: 1, MaxAttempts: 3, Seed: 1})
	err := l.Deliver(context.Background(), EndpointA, EndpointB, hello())
	if qkderr.KindOf(err) != qkderr.KindDropped {
		t.Fatalf("kind = %s, want dropped", qkderr.KindOf(err))
	}
}

func TestDeliver_BadVersion_Classified(t *testing.T) {
	l := New(Config{Seed: 1})
	m := hello()
	m.Version = "qkd-sim/9"
	err := l.Deliver(context.Background(), EndpointA, EndpointB, m)
	if qkderr.KindOf(err) != qkderr.KindUnsupportedVersion {
		t.Fatalf("kind = %s, want unsupported-version", qkderr.KindOf(err))
	}
}

func TestRecv_Timeout(t *testing.T) {
	l := New(Config{Seed: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := l.Recv(ctx, EndpointA); qkderr.KindOf(err) != qkderr.KindTimeout {
		t.Fatalf("kind = %s, want timeout", qkderr.KindOf(err))
	}
}
