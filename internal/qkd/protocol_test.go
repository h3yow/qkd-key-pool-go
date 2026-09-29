package qkd

import (
	"errors"
	"testing"
	"time"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf(format, args...)
}

func TestOrderGuardAcceptsStrictSequence(t *testing.T) {
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	g := NewOrderGuard(0)
	for seq := uint64(0); seq < 5; seq++ {
		m := &Message{Version: ProtocolVersion1, Type: MsgHello, Seq: seq, IssuedAt: base, TTL: time.Second}
		if err := g.Check(m, base); err != nil {
			t.Fatalf("seq=%d expected accept, got %v", seq, err)
		}
	}
	if g.NextSeq() != 5 {
		t.Fatalf("want nextSeq=5, got %d", g.NextSeq())
	}
}

func TestOrderGuardRejectCausesAreDistinguishable(t *testing.T) {
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	now := base.Add(500 * time.Millisecond)

	cases := []struct {
		name string
		msg  *Message
		// 在 Check 之前先把守卫推进到 seq=2
		advance bool
		want    RejectCode
	}{
		{
			name: "version incompatible",
			msg:  &Message{Version: "QKD/9.9", Type: MsgHello, Seq: 0, IssuedAt: base, TTL: time.Second},
			want: RejectVersionIncompat,
		},
		{
			name:    "duplicate old seq",
			msg:     &Message{Version: ProtocolVersion1, Type: MsgHello, Seq: 1, IssuedAt: base, TTL: time.Second},
			advance: true,
			want:    RejectDuplicate,
		},
		{
			name:    "out of order gap",
			msg:     &Message{Version: ProtocolVersion1, Type: MsgHello, Seq: 5, IssuedAt: base, TTL: time.Second},
			advance: true,
			want:    RejectOutOfOrder,
		},
		{
			name: "expired ttl",
			msg:  &Message{Version: ProtocolVersion1, Type: MsgHello, Seq: 0, IssuedAt: base, TTL: 100 * time.Millisecond},
			want: RejectExpired,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := NewOrderGuard(0)
			if tc.advance {
				for _, seq := range []uint64{0, 1} {
					m := &Message{Version: ProtocolVersion1, Seq: seq, IssuedAt: base, TTL: time.Hour}
					if err := g.Check(m, now); err != nil {
						t.Fatalf("advance seq=%d: %v", seq, err)
					}
				}
			}
			err := g.Check(tc.msg, now)
			var rj *RejectError
			if !errors.As(err, &rj) {
				t.Fatalf("want *RejectError, got %T: %v", err, err)
			}
			if rj.Code != tc.want {
				t.Fatalf("want reject code=%s, got=%s (detail=%s)", tc.want, rj.Code, rj.Detail)
			}
			t.Logf("input seq=%d version=%s ttl=%s -> reject %s, wantSeq=%d",
				tc.msg.Seq, tc.msg.Version, tc.msg.TTL, rj.Code, rj.WantSeq)
		})
	}
}

func TestRejectedMessageMustNotAdvanceGuard(t *testing.T) {
	// 被拒消息绝不能悄悄吞掉序号窗口：版本不兼容之后，正确消息仍按原期望序号被接受。
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	g := NewOrderGuard(0)
	bad := &Message{Version: "QKD/9.9", Seq: 0, IssuedAt: base, TTL: time.Second}
	if err := g.Check(bad, base); err == nil {
		t.Fatal("expected rejection for bad version")
	}
	if g.NextSeq() != 0 {
		t.Fatalf("guard must not advance on reject, got next=%d", g.NextSeq())
	}
	good := &Message{Version: ProtocolVersion1, Seq: 0, IssuedAt: base, TTL: time.Second}
	if err := g.Check(good, base); err != nil {
		t.Fatalf("corrected message must be accepted: %v", err)
	}
}
