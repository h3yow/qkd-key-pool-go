// Package pool implements a local QKD key pool with leased key material:
// lease (exclusive hold), confirm (consume), release (return), and timed
// expiry/reclamation. All material is locally generated fake randomness; no
// real device or secret is involved.
//
// Concurrency contract: at most one lease can ever be granted for a given key;
// consumed and expired keys are never handed out again. Every operation is
// guarded so concurrent callers observe an explainable, classified outcome.
package pool

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

// KeyState is the lifecycle state of a single key entry.
type KeyState int

const (
	KeyAvailable KeyState = iota
	KeyLeased
	KeyConsumed
	KeyExpired
)

var keyStateNames = [...]string{"available", "leased", "consumed", "expired"}

func (k KeyState) String() string {
	if int(k) < 0 || int(k) >= len(keyStateNames) {
		return "unknown"
	}
	return keyStateNames[k]
}

// Key is one pool entry. Its mutable state is owned by Pool under its lock;
// callers read the snapshot returned by Pool.
type Key struct {
	ID       string
	Material []byte
	state    KeyState
	expiry   time.Time
	leasedAt time.Time
	leaseID  string
}

// State returns the lifecycle state.
func (k *Key) State() KeyState { return k.state }

// Expiry returns the absolute material expiry.
func (k *Key) Expiry() time.Time { return k.expiry }

// Lease is an exclusive, time-bounded hold on a key.
type Lease struct {
	ID      string
	KeyID   string
	expires time.Time
}

// Expires returns the lease deadline.
func (l *Lease) Expires() time.Time { return l.expires }

// Config configures the pool.
type Config struct {
	LeaseTTL     time.Duration
	ReapInterval time.Duration
	Now          func() time.Time // injectable clock for deterministic tests
}

// Pool is the concurrency-safe key store.
type Pool struct {
	cfg Config
	mu  sync.Mutex
	now func() time.Time

	keys   map[string]*Key
	leases map[string]*Lease
	// leaseToKey maps lease ID -> key ID for reverse lookup.
	leaseToKey map[string]string

	reaperStop chan struct{}
	reaperDone chan struct{}
}

// New constructs an empty pool with applied defaults.
func New(cfg Config) *Pool {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 2 * time.Second
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 100 * time.Millisecond
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Pool{
		cfg:        cfg,
		now:        now,
		keys:       map[string]*Key{},
		leases:     map[string]*Lease{},
		leaseToKey: map[string]string{},
		reaperStop: make(chan struct{}),
		reaperDone: make(chan struct{}),
	}
}

// nowOr returns the injected/current time.
func (p *Pool) t() time.Time { return p.now() }

// Add inserts a pre-generated key with an absolute expiry. It is a test/seed
// helper; expired material is accepted but marked expired immediately.
func (p *Pool) Add(id string, material []byte, expiry time.Time) error {
	if id == "" {
		return qkderr.New(qkderr.KindMalformed, "empty key id")
	}
	if len(material) == 0 {
		return qkderr.New(qkderr.KindMalformed, "empty key material")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.keys[id]; exists {
		return qkderr.New(qkderr.KindDuplicateMessage, "key id already present")
	}
	st := KeyAvailable
	if !expiry.After(p.t()) {
		st = KeyExpired
	}
	p.keys[id] = &Key{
		ID: id, Material: append([]byte(nil), material...),
		state: st, expiry: expiry,
	}
	return nil
}

// Generate adds n fresh keys of size bytes each with a material lifetime.
func (p *Pool) Generate(n, size int, lifetime time.Duration) error {
	for i := 0; i < n; i++ {
		mat := make([]byte, size)
		if _, err := rand.Read(mat); err != nil {
			// Do not leak crypto/rand internals; classify as internal.
			return qkderr.Wrap(qkderr.KindInternal, "key generation failed", err)
		}
		id := "key-" + hex.EncodeToString(mat[:8])
		if err := p.Add(id, mat, p.t().Add(lifetime)); err != nil {
			return err
		}
	}
	return nil
}
