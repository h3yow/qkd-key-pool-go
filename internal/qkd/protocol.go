package qkd

import (
	"fmt"
	"time"
)

// Version 是协议版本标识。
type Version string

const (
	ProtocolVersion1 Version = "QKD/1.0"
)

func SupportedVersions() []Version {
	return []Version{ProtocolVersion1}
}

func IsSupported(v Version) bool {
	for _, sv := range SupportedVersions() {
		if sv == v {
			return true
		}
	}
	return false
}

// MessageType 标识协商消息类型。
type MessageType string

const (
	MsgHello      MessageType = "hello"
	MsgParameters MessageType = "parameters"
	MsgBases      MessageType = "bases"
	MsgSift       MessageType = "sift"
	MsgQBER       MessageType = "qber"
	MsgConfirm    MessageType = "confirm"
	MsgConfirmAck MessageType = "confirm_ack"
	MsgAbort      MessageType = "abort"
)

// Message 是协商链路上的一条控制消息。
type Message struct {
	Version   Version
	Type      MessageType
	SessionID string
	Seq       uint64
	IssuedAt  time.Time
	TTL       time.Duration
	Payload   []byte
}

// Expiry 返回消息过期时刻；TTL<=0 表示永不过期。
func (m *Message) Expiry() time.Time {
	if m.TTL <= 0 {
		return time.Time{}
	}
	return m.IssuedAt.Add(m.TTL)
}

// Expired 判断消息在 now 时刻是否已超过 TTL。
func (m *Message) Expired(now time.Time) bool {
	if m.TTL <= 0 {
		return false
	}
	return now.After(m.IssuedAt.Add(m.TTL))
}

// RejectCode 是入站消息被拒收的可区分原因。
type RejectCode string

const (
	RejectVersionIncompat RejectCode = "version_incompatible"
	RejectDuplicate       RejectCode = "duplicate_message"
	RejectOutOfOrder      RejectCode = "out_of_order"
	RejectExpired         RejectCode = "message_expired"
)

// RejectError 表示一条入站消息被明确拒收，绝不静默忽略。
type RejectError struct {
	Code    RejectCode
	Seq     uint64
	WantSeq uint64
	Detail  string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("qkd message rejected: code=%s seq=%d want=%d detail=%s",
		e.Code, e.Seq, e.WantSeq, e.Detail)
}

// OrderGuard 按严格递增序号跟踪对端消息：
//   - 小于 firstSeq 或已处理过的序号 -> 重复
//   - 跳过期望序号的新序号 -> 乱序
//
// 它不关心消息类型，也不修改消息内容。
type OrderGuard struct {
	next uint64
	seen map[uint64]struct{}
}

func NewOrderGuard(firstSeq uint64) *OrderGuard {
	return &OrderGuard{next: firstSeq, seen: make(map[uint64]struct{})}
}

// Check 依次做版本 -> 时效 -> 重复/乱序校验。
// 任一条件不满足返回 *RejectError；通过后推进期望序号。
// 版本不兼容与过期消息不推进守卫状态（对端可能重发修正版本/新消息）。
func (g *OrderGuard) Check(m *Message, now time.Time) error {
	if m == nil {
		return &RejectError{Code: RejectOutOfOrder, WantSeq: g.next, Detail: "nil message"}
	}
	if !IsSupported(m.Version) {
		return &RejectError{
			Code:    RejectVersionIncompat,
			Seq:     m.Seq,
			WantSeq: g.next,
			Detail:  fmt.Sprintf("version %q not in %v", m.Version, SupportedVersions()),
		}
	}
	if m.Expired(now) {
		return &RejectError{
			Code:    RejectExpired,
			Seq:     m.Seq,
			WantSeq: g.next,
			Detail:  fmt.Sprintf("issued_at=%s ttl=%s now=%s", m.IssuedAt.Format(time.RFC3339Nano), m.TTL, now.Format(time.RFC3339Nano)),
		}
	}
	if m.Seq < g.next {
		// 旧序号：处理过的与未来回绕的都按重复处理，绝不重复应用。
		return &RejectError{
			Code:    RejectDuplicate,
			Seq:     m.Seq,
			WantSeq: g.next,
			Detail:  "seq already applied or behind window",
		}
	}
	if m.Seq > g.next {
		return &RejectError{
			Code:    RejectOutOfOrder,
			Seq:     m.Seq,
			WantSeq: g.next,
			Detail:  "seq skipped expected sequence number",
		}
	}
	g.seen[m.Seq] = struct{}{}
	g.next++
	return nil
}

func (g *OrderGuard) NextSeq() uint64 { return g.next }
