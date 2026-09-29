package pool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrorCode 是密钥池操作的可区分错误码。
type ErrorCode string

const (
	ErrEmptyPool       ErrorCode = "empty_pool"       // 没有可租用的密钥
	ErrLeaseNotFound   ErrorCode = "lease_not_found"  // 租约不存在（已确认/释放/回收）
	ErrKeyNotLeased    ErrorCode = "key_not_leased"   // 目标密钥不在 leased 状态
	ErrKeyConsumed     ErrorCode = "key_consumed"     // 密钥已确认消费
	ErrKeyExpired      ErrorCode = "key_expired"      // 密钥已过期
	ErrLeaseExpired    ErrorCode = "lease_expired"    // 租约已过期
	ErrAlreadyReleased ErrorCode = "already_released" // 租约已释放
)

// Error 是密钥池返回的错误，只含 id/操作，绝不含密钥材料。
type Error struct {
	Code      ErrorCode
	LeaseID   string
	KeyID     string
	Operation string
}

func (e *Error) Error() string {
	return fmt.Sprintf("key pool error: op=%s code=%s lease=%s key=%s",
		e.Operation, e.Code, e.LeaseID, e.KeyID)
}

// Logger 与 qkd 包的日志接口同形，避免包间依赖。
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(format string, args ...any) {}

// Lease 是一次密钥租约。
type Lease struct {
	ID        string
	KeyID     string
	SessionID string
	GrantedAt time.Time
	ExpiresAt time.Time
}

// Config 是密钥池配置。
type Config struct {
	DefaultTTL   time.Duration // 新入池密钥的默认寿命，也是租约上限
	ReapInterval time.Duration // 后台回收间隔，0 表示默认 100ms
	Log          Logger
}

// Pool 管理密钥与租约，所有状态变更都在同一把锁内完成，
// 因此“并发抢同一把钥匙只有一个成功”由互斥天然保证。
type Pool struct {
	mu     sync.Mutex
	cfg    Config
	keys   map[string]*Key
	leases map[string]*Lease
	seq    uint64
}

func New(cfg Config) *Pool {
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 30 * time.Second
	}
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 100 * time.Millisecond
	}
	if cfg.Log == nil {
		cfg.Log = nopLogger{}
	}
	return &Pool{
		cfg:    cfg,
		keys:   make(map[string]*Key),
		leases: make(map[string]*Lease),
	}
}

func (p *Pool) logf(format string, args ...any) {
	p.cfg.Log.Printf("[pool] " + fmt.Sprintf(format, args...))
}

// AddKey 放入一份新密钥，材料被拷贝后持有。
func (p *Pool) AddKey(id string, material []byte, now time.Time) *Key {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := NewKey(id, material, now, p.cfg.DefaultTTL)
	p.keys[id] = k
	p.logf("add key id=%s bytes=%d expires_at=%s", id, len(material), k.ExpiresAt().Format(time.RFC3339Nano))
	return k
}

// Lease 原子地从“可用且未过期”的密钥中租用一份。
// 并发调用下，同一份密钥只会被一个会话租走，其余得到 empty_pool。
func (p *Pool) Lease(sessionID string, now time.Time) (*Lease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 顺手冻结已过期的可用密钥，使其不再参与分配。
	for _, k := range p.keys {
		if k.status == StatusAvailable && k.Expired(now) {
			k.status = StatusExpired
		}
	}

	ids := make([]string, 0, len(p.keys))
	for id, k := range p.keys {
		if k.status == StatusAvailable {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, &Error{Code: ErrEmptyPool, Operation: "lease"}
	}
	sort.Strings(ids)
	k := p.keys[ids[0]]

	p.seq++
	leaseID := fmt.Sprintf("lease-%d", p.seq)
	l := &Lease{
		ID:        leaseID,
		KeyID:     k.id,
		SessionID: sessionID,
		GrantedAt: now,
		ExpiresAt: k.expiresAt, // 租约不得长于密钥寿命
	}
	p.leases[leaseID] = l
	k.status = StatusLeased
	p.logf("lease granted id=%s key=%s session=%s expires_at=%s",
		leaseID, k.id, sessionID, l.ExpiresAt.Format(time.RFC3339Nano))
	return &Lease{
		ID:        l.ID,
		KeyID:     l.KeyID,
		SessionID: l.SessionID,
		GrantedAt: l.GrantedAt,
		ExpiresAt: l.ExpiresAt,
	}, nil
}

// activeLeaseLocked 取出仍有效的租约，过期租约就地回收并返回可区分错误。
func (p *Pool) activeLeaseLocked(leaseID, op string, now time.Time) (*Lease, *Key, error) {
	l, ok := p.leases[leaseID]
	if !ok {
		return nil, nil, &Error{Code: ErrLeaseNotFound, LeaseID: leaseID, Operation: op}
	}
	k := p.keys[l.KeyID]
	if now.After(l.ExpiresAt) || k.Expired(now) {
		delete(p.leases, leaseID)
		k.status = StatusExpired
		p.logf("lease expired on op=%s id=%s key=%s", op, leaseID, k.id)
		return nil, nil, &Error{Code: ErrLeaseExpired, LeaseID: leaseID, KeyID: k.id, Operation: op}
	}
	return l, k, nil
}

// Material 仅在租约有效期内返回密钥材料的副本；
// 确认/释放/回收之后无法再经此接口读到材料。
func (p *Pool) Material(leaseID string, now time.Time) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, k, err := p.activeLeaseLocked(leaseID, "material", now)
	if err != nil {
		return nil, err
	}
	return k.Material(), nil
}

// Confirm 确认消费：密钥进入 confirmed，租约注销，材料永不再发放。
func (p *Pool) Confirm(leaseID string, now time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, k, err := p.activeLeaseLocked(leaseID, "confirm", now)
	if err != nil {
		return err
	}
	_ = l
	switch k.status {
	case StatusConfirmed:
		return &Error{Code: ErrKeyConsumed, LeaseID: leaseID, KeyID: k.id, Operation: "confirm"}
	case StatusReleased:
		return &Error{Code: ErrAlreadyReleased, LeaseID: leaseID, KeyID: k.id, Operation: "confirm"}
	}
	k.status = StatusConfirmed
	delete(p.leases, leaseID)
	p.logf("lease confirmed id=%s key=%s -> confirmed (consumed)", leaseID, k.id)
	return nil
}

// Release 释放租约：密钥作废为 released（不回收复用），租约注销。
func (p *Pool) Release(leaseID string, now time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, k, err := p.activeLeaseLocked(leaseID, "release", now)
	if err != nil {
		return err
	}
	k.status = StatusReleased
	delete(p.leases, leaseID)
	p.logf("lease released id=%s key=%s -> released (destroyed)", leaseID, k.id)
	return nil
}

// ReapExpired 回收过期的可用密钥与活动租约，返回受影响条数。
// 状态全部落为 expired，保证异常退出/超时后的状态可解释。
func (p *Pool) ReapExpired(now time.Time) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, k := range p.keys {
		if (k.status == StatusAvailable || k.status == StatusLeased) && k.Expired(now) {
			k.status = StatusExpired
			n++
		}
	}
	for id, l := range p.leases {
		if now.After(l.ExpiresAt) {
			delete(p.leases, id)
			if k := p.keys[l.KeyID]; k.status == StatusLeased {
				k.status = StatusExpired
			}
			n++
			p.logf("reaper removed lease id=%s key=%s", id, l.KeyID)
		}
	}
	if n > 0 {
		p.logf("reaper reclaimed %d entries at %s", n, now.Format(time.RFC3339Nano))
	}
	return n
}

// RunReaper 按固定间隔运行回收，直到 ctx 取消。
func (p *Pool) RunReaper(ctx context.Context) {
	t := time.NewTicker(p.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			p.ReapExpired(now)
		}
	}
}

// KeyStatus 返回密钥当前状态；非终态但已过寿命的会实时折算为 expired。
func (p *Pool) KeyStatus(keyID string, now time.Time) (Status, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k, ok := p.keys[keyID]
	if !ok {
		return "", false
	}
	st := k.status
	if (st == StatusAvailable || st == StatusLeased) && k.Expired(now) {
		st = StatusExpired
	}
	return st, true
}

// LeaseInfo 返回租约快照；已注销的租约返回 false。
func (p *Pool) LeaseInfo(leaseID string) (*Lease, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, ok := p.leases[leaseID]
	if !ok {
		return nil, false
	}
	cp := *l
	return &cp, true
}
