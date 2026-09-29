package pool

import "time"

// Status 是密钥生命周期状态。
type Status string

const (
	StatusAvailable Status = "available" // 可被租用
	StatusLeased    Status = "leased"    // 已租给某会话，等待确认/释放
	StatusConfirmed Status = "confirmed" // 已确认消费，永不再发放
	StatusReleased  Status = "released"  // 租约释放，材料作废，不回收复用
	StatusExpired   Status = "expired"   // 已过期，由回收流程冻结
)

// Key 是一份受管密钥材料。字段为包内私有，外部只能拿到快照，
// 避免调用方绕过 Pool 直接改状态或持有/篡改底层材料切片。
type Key struct {
	id        string
	material  []byte
	createdAt time.Time
	expiresAt time.Time
	status    Status
}

func NewKey(id string, material []byte, now time.Time, ttl time.Duration) *Key {
	mat := make([]byte, len(material))
	copy(mat, material)
	return &Key{
		id:        id,
		material:  mat,
		createdAt: now,
		expiresAt: now.Add(ttl),
		status:    StatusAvailable,
	}
}

func (k *Key) ID() string { return k.id }

// Material 返回密钥材料的副本，杜绝外部与池内共享底层数组。
func (k *Key) Material() []byte {
	out := make([]byte, len(k.material))
	copy(out, k.material)
	return out
}

func (k *Key) Status() Status       { return k.status }
func (k *Key) ExpiresAt() time.Time { return k.expiresAt }

// Expired 在 now 严格晚于到期时刻时成立。
func (k *Key) Expired(now time.Time) bool { return now.After(k.expiresAt) }
