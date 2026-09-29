package pool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

// newLeaseID mints an opaque lease identifier from local randomness.
func newLeaseID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "lease-" + hex.EncodeToString(b[:])
}

// Lease grants an exclusive lease to one available, non-expired key. Exactly
// one of concurrent requests for the same key can win: keys are selected and
// flipped to KeyLeased atomically under the pool lock. Already-consumed and
// already-expired keys are never selected again.
func (p *Pool) Lease(ctx context.Context) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, qkderr.Wrap(qkderr.KindTimeout, "lease cancelled", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.t()
	for id, k := range p.keys {
		// Lazy material-expiry transition; such keys are skipped forever.
		if k.state == KeyAvailable && !k.expiry.After(now) {
			k.state = KeyExpired
		}
		if k.state != KeyAvailable {
			continue
		}
		l := &Lease{
			ID:      newLeaseID(),
			KeyID:   id,
			expires: now.Add(p.cfg.LeaseTTL),
		}
		k.state = KeyLeased
		k.leasedAt = now
		k.leaseID = l.ID
		p.leases[l.ID] = l
		p.leaseToKey[l.ID] = id
		return l, nil
	}
	return nil, qkderr.New(qkderr.KindKeyUnavailable,
		"no available (non-expired) key to lease")
}

// keyForLease must be called with p.mu held. It resolves a lease to its key
// and returns a classified error if the lease is unknown or already reaped.
func (p *Pool) keyForLease(leaseID string) (*Key, *Lease, error) {
	l, ok := p.leases[leaseID]
	if !ok {
		return nil, nil, qkderr.New(qkderr.KindLeaseNotHeld,
			"unknown or reclaimed lease")
	}
	k, ok := p.keys[p.leaseToKey[leaseID]]
	if !ok {
		return nil, nil, qkderr.Internal(nil)
	}
	return k, l, nil
}

// Confirm permanently consumes the key behind a currently-valid lease. A lease
// that has been reaped (TTL), released, or already confirmed is rejected with
// a distinguishable kind.
func (p *Pool) Confirm(ctx context.Context, leaseID string) error {
	if err := ctx.Err(); err != nil {
		return qkderr.Wrap(qkderr.KindTimeout, "confirm cancelled", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	k, _, err := p.keyForLease(leaseID)
	if err != nil {
		return err
	}
	now := p.t()
	switch {
	case !k.expiry.After(now):
		p.markExpiredLocked(k)
		return qkderr.New(qkderr.KindKeyExpired, "key material expired before confirm")
	case !p.leases[leaseID].expires.After(now):
		p.reapLeaseLocked(leaseID, k)
		return qkderr.New(qkderr.KindTimeout, "lease TTL expired before confirm")
	case k.state != KeyLeased:
		return qkderr.New(qkderr.KindInvalidState,
			"cannot confirm key in state "+k.state.String())
	}
	k.state = KeyConsumed
	p.removeLeaseLocked(leaseID)
	return nil
}

// Release returns a still-leased key to availability. Consumed/expired keys
// cannot be released.
func (p *Pool) Release(ctx context.Context, leaseID string) error {
	if err := ctx.Err(); err != nil {
		return qkderr.Wrap(qkderr.KindTimeout, "release cancelled", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	k, _, err := p.keyForLease(leaseID)
	if err != nil {
		return err
	}
	now := p.t()
	if !k.expiry.After(now) {
		p.markExpiredLocked(k)
		return qkderr.New(qkderr.KindKeyExpired, "key material expired")
	}
	if k.state != KeyLeased {
		return qkderr.New(qkderr.KindInvalidState,
			"cannot release key in state "+k.state.String())
	}
	k.state = KeyAvailable
	k.leaseID = ""
	p.removeLeaseLocked(leaseID)
	return nil
}

// removeLeaseLocked drops lease bookkeeping. Caller holds p.mu.
func (p *Pool) removeLeaseLocked(leaseID string) {
	delete(p.leases, leaseID)
	delete(p.leaseToKey, leaseID)
}

// reapLeaseLocked reclaims an expired lease, returning the key to the pool
// unless the material itself has expired. Caller holds p.mu.
func (p *Pool) reapLeaseLocked(leaseID string, k *Key) {
	if k.expiry.After(p.t()) {
		k.state = KeyAvailable
	} else {
		k.state = KeyExpired
	}
	k.leaseID = ""
	p.removeLeaseLocked(leaseID)
}

// markExpiredLocked expires a key and drops any lease bound to it.
func (p *Pool) markExpiredLocked(k *Key) {
	if k.leaseID != "" {
		p.removeLeaseLocked(k.leaseID)
	}
	k.state = KeyExpired
	k.leaseID = ""
}

// ReapOnce performs one reclamation pass: leases whose TTL elapsed are reaped
// (the key returns to the pool if its material is still valid), and keys whose
// material expired are marked expired. It returns the number of leases reaped.
func (p *Pool) ReapOnce(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, qkderr.Wrap(qkderr.KindTimeout, "reap cancelled", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.t()
	reaped := 0
	// Snapshot lease ids because the map mutates during reaping.
	var ids []string
	for id := range p.leases {
		ids = append(ids, id)
	}
	for _, id := range ids {
		k, ok := p.keys[p.leaseToKey[id]]
		if !ok {
			p.removeLeaseLocked(id)
			continue
		}
		if !k.expiry.After(now) {
			p.markExpiredLocked(k)
			reaped++
			continue
		}
		if l := p.leases[id]; l != nil && !l.expires.After(now) {
			p.reapLeaseLocked(id, k)
			reaped++
		}
	}
	// Also expire available keys past their material lifetime.
	for _, k := range p.keys {
		if (k.state == KeyAvailable || k.state == KeyLeased) && !k.expiry.After(now) {
			p.markExpiredLocked(k)
		}
	}
	return reaped, nil
}

// RunReaper starts background TTL reclamation at Config.ReapInterval until ctx
// is cancelled. It returns once the worker has stopped.
func (p *Pool) RunReaper(ctx context.Context) {
	go func() {
		t := time.NewTicker(p.cfg.ReapInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				select {
				case <-p.reaperDone:
				default:
					close(p.reaperDone)
				}
				return
			case <-t.C:
				_, _ = p.ReapOnce(ctx)
			}
		}
	}()
}

// ReaperDone closes when the background reaper has stopped.
func (p *Pool) ReaperDone() <-chan struct{} { return p.reaperDone }

// Len returns the counts of keys in each lifecycle state.
func (p *Pool) Len() (available, leased, consumed, expired int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, k := range p.keys {
		switch k.state {
		case KeyAvailable:
			available++
		case KeyLeased:
			leased++
		case KeyConsumed:
			consumed++
		case KeyExpired:
			expired++
		}
	}
	return
}

// KeyStateOf reports the current state of a key by ID.
func (p *Pool) KeyStateOf(id string) (KeyState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k, ok := p.keys[id]
	if !ok {
		return KeyExpired, qkderr.New(qkderr.KindKeyUnavailable, "unknown key id")
	}
	return k.state, nil
}

// ActiveLeases reports how many leases are currently held.
func (p *Pool) ActiveLeases() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.leases)
}
