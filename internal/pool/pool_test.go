package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type poolLogger struct{ t *testing.T }

func (l poolLogger) Printf(format string, args ...any) { l.t.Logf(format, args...) }

func testPool(t *testing.T, ttl time.Duration) (*Pool, time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	return New(Config{DefaultTTL: ttl, ReapInterval: time.Hour, Log: poolLogger{t: t}}), now
}

func wantPoolErr(t *testing.T, err error, code ErrorCode) *Error {
	t.Helper()
	var pe *Error
	if !errors.As(err, &pe) {
		t.Fatalf("want *Error code=%s, got %T: %v", code, err, err)
	}
	if pe.Code != code {
		t.Fatalf("want pool error=%s, got=%s", code, pe.Code)
	}
	return pe
}

func TestLeaseConfirmLifecycle(t *testing.T) {
	p, now := testPool(t, time.Minute)
	p.AddKey("k1", []byte("secret-material"), now)

	l, err := p.Lease("sess-A", now)
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	t.Logf("granted lease=%s key=%s", l.ID, l.KeyID)

	mat, err := p.Material(l.ID, now)
	if err != nil || string(mat) != "secret-material" {
		t.Fatalf("material during lease: %v %q", err, mat)
	}
	if err := p.Confirm(l.ID, now); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	st, _ := p.KeyStatus("k1", now)
	if st != StatusConfirmed {
		t.Fatalf("want confirmed, got %s", st)
	}
	// 已确认的租约不能重复确认，也读不到材料。
	wantPoolErr(t, p.Confirm(l.ID, now), ErrLeaseNotFound)
	_, err = p.Material(l.ID, now)
	wantPoolErr(t, err, ErrLeaseNotFound)
	// 池里不再有可用密钥。
	if _, err := p.Lease("sess-B", now); err == nil {
		t.Fatal("consumed key must never be reissued")
	} else {
		wantPoolErr(t, err, ErrEmptyPool)
	}
}

func TestReleaseDestroysKey(t *testing.T) {
	p, now := testPool(t, time.Minute)
	p.AddKey("k1", []byte("abc"), now)
	l, _ := p.Lease("sess-A", now)
	if err := p.Release(l.ID, now); err != nil {
		t.Fatalf("release: %v", err)
	}
	if st, _ := p.KeyStatus("k1", now); st != StatusReleased {
		t.Fatalf("want released, got %s", st)
	}
	wantPoolErr(t, p.Confirm(l.ID, now), ErrLeaseNotFound)
}

func TestConcurrentLeaseOnlyOneWinner(t *testing.T) {
	p, now := testPool(t, time.Hour)
	p.AddKey("only-key", []byte("lonely"), now)

	const n = 16
	var wg sync.WaitGroup
	results := make(chan error, n)
	leaseIDs := make(chan string, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			l, err := p.Lease("sess", now)
			if err == nil {
				leaseIDs <- l.ID
			}
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(leaseIDs)

	winners := 0
	empty := 0
	for err := range results {
		if err == nil {
			winners++
		} else {
			empty++
			wantPoolErr(t, err, ErrEmptyPool)
		}
	}
	ids := map[string]struct{}{}
	for id := range leaseIDs {
		ids[id] = struct{}{}
	}
	if winners != 1 || empty != n-1 || len(ids) != 1 {
		t.Fatalf("want exactly 1 winner, got winners=%d empty=%d distinctLeases=%d", winners, empty, len(ids))
	}
	t.Logf("concurrency=%d winners=%d losers(empty_pool)=%d", n, winners, empty)
}

func TestLeaseExpiryReapAndMaterialDenied(t *testing.T) {
	p, now := testPool(t, time.Minute)
	p.AddKey("k1", []byte("ephemeral"), now)
	l, _ := p.Lease("sess-A", now)

	// TTL 内一切正常。
	if _, err := p.Material(l.ID, now.Add(30*time.Second)); err != nil {
		t.Fatalf("material before expiry: %v", err)
	}

	// 过期后：主动操作直接报 lease_expired，状态落 expired。
	_, err := p.Material(l.ID, now.Add(61*time.Second))
	wantPoolErr(t, err, ErrLeaseExpired)
	if st, _ := p.KeyStatus("k1", now.Add(61*time.Second)); st != StatusExpired {
		t.Fatalf("want expired, got %s", st)
	}
	// 过期密钥不得再被租出。
	if _, err := p.Lease("sess-B", now.Add(61*time.Second)); err == nil {
		t.Fatal("expired key must not be leased")
	}

	// 第二把钥匙：验证后台回收器的批量回收计数。
	p2, n2 := testPool(t, 10*time.Second)
	p2.AddKey("k2", []byte("x"), n2)
	if n := p2.ReapExpired(n2.Add(5 * time.Second)); n != 0 {
		t.Fatalf("nothing should be reaped before ttl, got %d", n)
	}
	if n := p2.ReapExpired(n2.Add(11 * time.Second)); n != 1 {
		t.Fatalf("want 1 reaped entry, got %d", n)
	}
}

func TestReaperRunsInBackground(t *testing.T) {
	// 后台回收器使用墙钟，因此这里用真实时间验证。
	now := time.Now()
	p := New(Config{
		DefaultTTL:   40 * time.Millisecond,
		ReapInterval: 10 * time.Millisecond,
		Log:          poolLogger{t: t},
	})
	p.AddKey("k1", []byte("y"), now)
	l, err := p.Lease("sess-A", now)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go p.RunReaper(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := p.LeaseInfo(l.ID); !ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if _, ok := p.LeaseInfo(l.ID); ok {
		t.Fatal("background reaper must remove expired lease")
	}
	if st, _ := p.KeyStatus("k1", now.Add(time.Hour)); st != StatusExpired {
		t.Fatalf("want expired after reaper, got %s", st)
	}
}

func TestExpiredAvailableKeyNotLeased(t *testing.T) {
	p, now := testPool(t, time.Minute)
	p.AddKey("old", []byte("old"), now)
	later := now.Add(61 * time.Second)
	_, err := p.Lease("s", later)
	wantPoolErr(t, err, ErrEmptyPool)
	if st, _ := p.KeyStatus("old", later); st != StatusExpired {
		t.Fatalf("want expired, got %s", st)
	}
}

func TestMaterialIsolation(t *testing.T) {
	p, now := testPool(t, time.Minute)
	p.AddKey("k1", []byte("abcdef"), now)
	l, _ := p.Lease("s", now)
	m, _ := p.Material(l.ID, now)
	m[0] = 'X' // 调用方篡改副本
	m2, _ := p.Material(l.ID, now)
	if string(m2) != "abcdef" {
		t.Fatalf("pool material must be isolated from caller slices, got %q", m2)
	}
}
