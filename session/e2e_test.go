package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/qkd-key-pool-go/link"
	"github.com/h3yow/qkd-key-pool-go/pool"
	"github.com/h3yow/qkd-key-pool-go/qkderr"
	"github.com/h3yow/qkd-key-pool-go/session"
)

type tlog struct{ t *testing.T }

func (l tlog) Printf(format string, args ...any) { l.t.Logf("[e2e] "+format, args...) }

// negotiateAndLease runs a session and, only on clean establishment, leases a
// key from the pool. It returns the negotiation error and the lease error so a
// test can assert that keys follow successful negotiation.
func negotiateAndLease(t *testing.T, sCfg session.Config, p *pool.Pool) (sErr, lErr error) {
	t.Helper()
	n := session.New(sCfg, tlog{t})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sErr = n.Run(ctx)
	if sErr != nil {
		return sErr, nil
	}
	if n.State() != session.StateEstablished {
		t.Fatalf("nil error but state=%s", n.State())
	}
	_, lErr = p.Lease(ctx)
	return sErr, lErr
}

func TestE2E_SuccessThenLeaseAndConfirm(t *testing.T) {
	clk := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	p := pool.New(pool.Config{
		LeaseTTL: time.Minute,
		Now:      func() time.Time { return clk },
	})
	if err := p.Add("secret-1", []byte("0123456789ABCDEF"), clk.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	sCfg := session.Config{
		SessionID: "e2e-ok", QBERThreshold: 0.11,
		Link: link.Config{BaseQBER: 0.02, Disturbance: 0.01, Seed: 1},
	}
	n := session.New(sCfg, tlog{t})
	ctx := context.Background()
	if err := n.Run(ctx); err != nil {
		t.Fatalf("negotiation: %v", err)
	}
	lease, err := p.Lease(ctx)
	if err != nil {
		t.Fatalf("lease after established session: %v", err)
	}
	if err := p.Confirm(ctx, lease.ID); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	avail, leased, consumed, expired := p.Len()
	t.Logf("pool counts avail=%d leased=%d consumed=%d expired=%d", avail, leased, consumed, expired)
	if consumed != 1 {
		t.Fatalf("consumed = %d, want 1", consumed)
	}
}

func TestE2E_HighQBER_KeyNotIssued(t *testing.T) {
	clk := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	p := pool.New(pool.Config{LeaseTTL: time.Minute, Now: func() time.Time { return clk }})
	_ = p.Add("secret-1", []byte("0123456789ABCDEF"), clk.Add(time.Hour))

	sCfg := session.Config{
		SessionID: "e2e-qber", QBERThreshold: 0.11,
		Link: link.Config{BaseQBER: 0.02, Disturbance: 0.3, Seed: 1},
	}
	sErr, _ := negotiateAndLease(t, sCfg, p)
	if qkderr.KindOf(sErr) != qkderr.KindQBERTooHigh {
		t.Fatalf("session kind = %s, want qber-too-high", qkderr.KindOf(sErr))
	}
	// Pool must still hold its single key available; nothing was consumed.
	avail, leased, consumed, _ := p.Len()
	t.Logf("after aborted session pool avail=%d leased=%d consumed=%d", avail, leased, consumed)
	if avail != 1 || leased != 0 || consumed != 0 {
		t.Fatalf("counts avail=%d leased=%d consumed=%d, want 1/0/0", avail, leased, consumed)
	}
}
