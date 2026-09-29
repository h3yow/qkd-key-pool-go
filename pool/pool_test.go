package pool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestPool(t *testing.T, ttl time.Duration) (*Pool, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)}
	p := New(Config{
		LeaseTTL:     ttl,
		ReapInterval: 10 * time.Millisecond,
		Now:          clk.now,
	})
	return p, clk
}

func seedKey(t *testing.T, p *Pool) string {
	t.Helper()
	if err := p.Add("k1", []byte("0123456789abcdef"),
		p.now().Add(time.Hour)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return "k1"
}

func TestLease_Confirm_ConsumedNotReissued(t *testing.T) {
	p, _ := newTestPool(t, time.Minute)
	seedKey(t, p)

	l, err := p.Lease(context.Background())
	if err != nil {
		t.Fatalf("first lease: %v", err)
	}
	t.Logf("leased key=%s lease=%s", l.KeyID, l.ID)

	// No available key remains -> second lease fails distinctly.
	if _, err := p.Lease(context.Background()); qkderr.KindOf(err) != qkderr.KindKeyUnavailable {
		t.Fatalf("second lease kind = %s, want key-unavailable", qkderr.KindOf(err))
	}

	if err := p.Confirm(context.Background(), l.ID); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	st, _ := p.KeyStateOf("k1")
	t.Logf("after confirm state=%s", st)
	if st != KeyConsumed {
		t.Fatalf("state = %s, want consumed", st)
	}

	// Consumed key must never be handed out again.
	if _, err := p.Lease(context.Background()); qkderr.KindOf(err) != qkderr.KindKeyUnavailable {
		t.Fatalf("post-consume lease kind = %s, want key-unavailable", qkderr.KindOf(err))
	}
	// A stale/duplicate confirm on the same lease is rejected.
	if err := p.Confirm(context.Background(), l.ID); qkderr.KindOf(err) != qkderr.KindLeaseNotHeld {
		t.Fatalf("double confirm kind = %s, want lease-not-held", qkderr.KindOf(err))
	}
}

func TestConcurrentLease_OnlyOneWinner(t *testing.T) {
	p, _ := newTestPool(t, time.Minute)
	seedKey(t, p)

	const n = 64
	var wg sync.WaitGroup
	var mu sync.Mutex
	var winners, losers int
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			l, err := p.Lease(context.Background())
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners++
				t.Logf("winner lease=%s key=%s", l.ID, l.KeyID)
			} else if qkderr.KindOf(err) == qkderr.KindKeyUnavailable {
				losers++
			}
		}()
	}
	wg.Wait()
	t.Logf("winners=%d losers=%d activeLeases=%d", winners, losers, p.ActiveLeases())
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
	if losers != n-1 {
		t.Fatalf("losers = %d, want %d", losers, n-1)
	}
	avail, leased, consumed, expired := p.Len()
	if leased != 1 || avail|consumed|expired != 0 {
		t.Fatalf("counts avail=%d leased=%d consumed=%d expired=%d", avail, leased, consumed, expired)
	}
}

func TestLeaseTTL_ReapedAndKeyReturns(t *testing.T) {
	p, clk := newTestPool(t, 100*time.Millisecond)
	seedKey(t, p)

	l, err := p.Lease(context.Background())
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	// Before TTL: still leased, and confirm on another lease is rejected.
	clk.advance(50 * time.Millisecond)
	if st, _ := p.KeyStateOf("k1"); st != KeyLeased {
		t.Fatalf("pre-TTL state = %s, want leased", st)
	}
	// Expire the lease.
	clk.advance(60 * time.Millisecond)
	n, err := p.ReapOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("reap n=%d err=%v, want 1", n, err)
	}
	t.Logf("reaped leases=%d", n)
	if st, _ := p.KeyStateOf("k1"); st != KeyAvailable {
		t.Fatalf("after reap state = %s, want available (returned)", st)
	}
	// The old lease is no longer usable: cannot confirm/release it.
	if err := p.Confirm(context.Background(), l.ID); qkderr.KindOf(err) != qkderr.KindLeaseNotHeld {
		t.Fatalf("confirm reaped lease kind = %s, want lease-not-held", qkderr.KindOf(err))
	}
	// A fresh lease can now grab the returned key.
	l2, err := p.Lease(context.Background())
	if err != nil {
		t.Fatalf("re-lease: %v", err)
	}
	if l2.KeyID != "k1" {
		t.Fatalf("re-leased key = %s, want k1", l2.KeyID)
	}
}

func TestConfirmAfterTTL_Rejected(t *testing.T) {
	p, clk := newTestPool(t, 50*time.Millisecond)
	seedKey(t, p)
	l, _ := p.Lease(context.Background())
	clk.advance(60 * time.Millisecond)
	err := p.Confirm(context.Background(), l.ID)
	t.Logf("late confirm -> kind=%s (%v)", qkderr.KindOf(err), err)
	if qkderr.KindOf(err) != qkderr.KindTimeout {
		t.Fatalf("kind = %s, want timeout (lease TTL)", qkderr.KindOf(err))
	}
}

func TestExpiredMaterial_NeverLeased(t *testing.T) {
	p, clk := newTestPool(t, time.Minute)
	if err := p.Add("old", []byte("expired-bytes!!"), clk.now().Add(-time.Second)); err != nil {
		t.Fatalf("add expired: %v", err)
	}
	_, err := p.Lease(context.Background())
	if qkderr.KindOf(err) != qkderr.KindKeyUnavailable {
		t.Fatalf("lease expired material kind = %s, want key-unavailable", qkderr.KindOf(err))
	}
	st, _ := p.KeyStateOf("old")
	if st != KeyExpired {
		t.Fatalf("state = %s, want expired", st)
	}
}

func TestRelease_ReturnsKey(t *testing.T) {
	p, _ := newTestPool(t, time.Minute)
	seedKey(t, p)
	l, _ := p.Lease(context.Background())
	if err := p.Release(context.Background(), l.ID); err != nil {
		t.Fatalf("release: %v", err)
	}
	if st, _ := p.KeyStateOf("k1"); st != KeyAvailable {
		t.Fatalf("state = %s, want available", st)
	}
	// Releasing an unknown lease is a distinguishable rejection.
	if err := p.Release(context.Background(), l.ID); qkderr.KindOf(err) != qkderr.KindLeaseNotHeld {
		t.Fatalf("re-release kind = %s, want lease-not-held", qkderr.KindOf(err))
	}
}

func TestBackgroundReaper_RecoversLease(t *testing.T) {
	p, clk := newTestPool(t, 20*time.Millisecond)
	seedKey(t, p)
	_, _ = p.Lease(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	p.RunReaper(ctx)
	clk.advance(40 * time.Millisecond)

	// Wait until the background reaper observes the TTL (poll the state).
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if st, _ := p.KeyStateOf("k1"); st == KeyAvailable {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st, _ := p.KeyStateOf("k1"); st != KeyAvailable {
		t.Fatalf("background reaper did not return key, state=%s", st)
	}
	cancel()
	<-p.ReaperDone()
}
