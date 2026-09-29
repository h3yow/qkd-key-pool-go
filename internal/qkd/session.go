package qkd

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// State 是协商会话状态。
type State string

const (
	StateIdle         State = "idle"
	StateHello        State = "hello"
	StateParameters   State = "parameters"
	StateTransmission State = "quantum_transmission"
	StateSifting      State = "sifting"
	StateEstimating   State = "estimating_qber"
	StateConfirming   State = "confirming"
	StateEstablished  State = "established"
	StateAborted      State = "aborted"
)

// Logger 是会话使用的最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(format string, args ...any) {}

// NopLogger 返回不输出的日志器。
func NopLogger() Logger { return nopLogger{} }

// SessionConfig 是协商配置。
type SessionConfig struct {
	Version       Version
	PhotonCount   int
	QBERThreshold float64
	Link          LinkConfig
	MessageTTL    time.Duration

	// 故障注入
	ForceConfirmFailure bool    // 确认阶段对端不回 ack
	PeerAbortAfter      State   // 对端在进入该状态后中止；空串表示不中止
	PeerVersion         Version // 对端通告版本，用于版本不兼容场景

	Seed int64
	Now  func() time.Time
	Log  Logger
}

// StepEvent 记录一次状态变化及其判定依据。
type StepEvent struct {
	At    time.Time
	State State
	Note  string
}

// Session 是一次本地模拟的 QKD 协商（同一进程内模拟 Alice 与 Bob）。
type Session struct {
	id        string
	cfg       SessionConfig
	state     State
	events    []StepEvent
	guard     *OrderGuard
	seq       uint64
	estimated *float64
}

func NewSession(id string, cfg SessionConfig) *Session {
	if cfg.Version == "" {
		cfg.Version = ProtocolVersion1
	}
	if cfg.PeerVersion == "" {
		cfg.PeerVersion = cfg.Version
	}
	if cfg.PhotonCount <= 0 {
		cfg.PhotonCount = 1024
	}
	if cfg.QBERThreshold <= 0 {
		cfg.QBERThreshold = 0.15
	}
	if cfg.MessageTTL <= 0 {
		cfg.MessageTTL = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Log == nil {
		cfg.Log = NopLogger()
	}
	return &Session{
		id:     id,
		cfg:    cfg,
		state:  StateIdle,
		guard:  NewOrderGuard(0),
		events: make([]StepEvent, 0, 16),
	}
}

// Result 是协商终态结果。
type Result struct {
	SessionID string
	State     State
	QBER      float64
	SharedKey []byte
	Events    []StepEvent
}

func (s *Session) State() State { return s.state }

func (s *Session) Events() []StepEvent { return s.events }

func (s *Session) now() time.Time { return s.cfg.Now() }

func (s *Session) logf(format string, args ...any) {
	s.cfg.Log.Printf("[session=%s] %s", s.id, fmt.Sprintf(format, args...))
}

// transition 推进状态并留痕：状态变化是排障与测试断言的主要依据。
func (s *Session) transition(st State, format string, args ...any) {
	note := fmt.Sprintf(format, args...)
	s.state = st
	s.events = append(s.events, StepEvent{At: s.now(), State: st, Note: note})
	s.logf("state -> %s | %s", st, note)
}

// fail 停到 aborted 状态，并把中止原因留痕。
func (s *Session) fail(f *Failure) (*Result, error) {
	s.state = StateAborted
	s.events = append(s.events, StepEvent{At: s.now(), State: StateAborted, Note: f.Error()})
	s.logf("ABORT at %s: %s", f.Step, f.Error())
	return &Result{SessionID: s.id, State: s.state, QBER: s.qberOrNaN(), Events: s.events}, f
}

func (s *Session) qberOrNaN() float64 {
	if s.estimated != nil {
		return *s.estimated
	}
	return -1
}

// peerMessage 构造一条对端控制消息，序号由会话单调分配。
func (s *Session) peerMessage(typ MessageType) *Message {
	m := &Message{
		Version:   s.cfg.PeerVersion,
		Type:      typ,
		SessionID: s.id,
		Seq:       s.seq,
		IssuedAt:  s.now(),
		TTL:       s.cfg.MessageTTL,
	}
	s.seq++
	return m
}

// recv 在期望状态 st 上接收对端消息，并做版本/时效/重复/乱序校验。
// PeerAbortAfter==st 时对端改发 abort；任何拒收都不静默忽略。
func (s *Session) recv(st State, want MessageType) error {
	typ := want
	if s.cfg.PeerAbortAfter == st {
		typ = MsgAbort
	}
	m := s.peerMessage(typ)
	if err := s.guard.Check(m, s.now()); err != nil {
		rj := err.(*RejectError)
		s.logf("inbound %s rejected at %s: %v", typ, st, rj)
		if rj.Code == RejectVersionIncompat {
			return NewFailure(FailureVersionIncompat, st, "peer advertised incompatible protocol version", err)
		}
		// 自驱动会话里出现重复/乱序/过期属于内部不变量被破坏，不向外暴露细节。
		return NewFailure(FailureInternal, st, "inbound control message rejected", err)
	}
	if typ == MsgAbort {
		return NewFailure(FailurePeerAborted, st, fmt.Sprintf("peer aborted before %s", want), nil)
	}
	return nil
}

// Run 执行完整协商；失败时返回 *Failure，调用方据 Code/Step 区分卡点。
func (s *Session) Run(ctx context.Context) (*Result, error) {
	s.estimated = nil
	checkCtx := func() error {
		if err := ctx.Err(); err != nil {
			return NewFailure(FailureInternal, s.state, "negotiation canceled", err)
		}
		return nil
	}

	// 1) hello：版本协商
	s.transition(StateHello, "send hello version=%s", s.cfg.Version)
	if err := s.recv(StateHello, MsgHello); err != nil {
		return s.fail(err.(*Failure))
	}
	if err := checkCtx(); err != nil {
		return s.fail(err.(*Failure))
	}

	// 2) parameters：确认链路与统计参数
	s.transition(StateParameters,
		"agree parameters photons=%d qber_threshold=%.4f loss=%.3f noise=%.3f ttl=%s",
		s.cfg.PhotonCount, s.cfg.QBERThreshold,
		s.cfg.Link.LossRate, s.cfg.Link.NoiseRate, s.cfg.MessageTTL)
	if err := s.recv(StateParameters, MsgParameters); err != nil {
		return s.fail(err.(*Failure))
	}

	// 3) 量子传输：Alice 制备光子，经扰动链路送达 Bob
	s.transition(StateTransmission, "alice prepares %d photons", s.cfg.PhotonCount)
	aliceRng := rand.New(rand.NewSource(s.cfg.Seed))
	bobRng := rand.New(rand.NewSource(s.cfg.Seed + 1))
	photons := make([]Photon, s.cfg.PhotonCount)
	bobBases := make([]byte, s.cfg.PhotonCount)
	for i := range photons {
		photons[i] = Photon{Bit: byte(aliceRng.Intn(2)), Basis: byte(aliceRng.Intn(2))}
		bobBases[i] = byte(bobRng.Intn(2))
	}
	ch := NewChannel(s.cfg.Link)
	delivered, delivIdx, rep := ch.TransmitWithIndices(photons)
	s.logf("link report sent=%d delivered=%d lost=%d flipped=%d",
		rep.Sent, rep.Delivered, rep.Lost, rep.Flipped)
	if err := checkCtx(); err != nil {
		return s.fail(err.(*Failure))
	}

	// 4) sifting：交换测量基，仅保留送达且基一致的比特
	s.transition(StateSifting, "exchange bases and sift")
	if err := s.recv(StateSifting, MsgBases); err != nil {
		return s.fail(err.(*Failure))
	}
	type siftedBit struct {
		alice byte
		bob   byte
	}
	sifted := make([]siftedBit, 0, len(delivered))
	for n, ph := range delivered {
		idx := delivIdx[n]
		if photons[idx].Basis != bobBases[idx] {
			continue
		}
		sifted = append(sifted, siftedBit{alice: photons[idx].Bit, bob: ph.Bit})
	}
	if err := s.recv(StateSifting, MsgSift); err != nil {
		return s.fail(err.(*Failure))
	}
	if len(sifted) == 0 {
		return s.fail(NewFailure(FailureNoKeyAgreed, StateSifting,
			fmt.Sprintf("no surviving sifted bits (delivered=%d)", rep.Delivered), nil))
	}

	// 5) 误码估计：基一致位置上的比特分歧率
	errors := 0
	for _, b := range sifted {
		if b.alice != b.bob {
			errors++
		}
	}
	qber := float64(errors) / float64(len(sifted))
	s.estimated = &qber
	s.transition(StateEstimating, "qber=%d/%d=%.4f threshold=%.4f",
		errors, len(sifted), qber, s.cfg.QBERThreshold)
	if err := s.recv(StateEstimating, MsgQBER); err != nil {
		return s.fail(err.(*Failure))
	}
	if qber > s.cfg.QBERThreshold {
		return s.fail(NewFailure(FailureQBERTooHigh, StateEstimating,
			fmt.Sprintf("qber %.4f exceeds threshold %.4f", qber, s.cfg.QBERThreshold), nil))
	}

	// 6) confirmation：密钥校验与确认；注入确认失败或对端中止
	s.transition(StateConfirming, "sifted key length=%d bits, send confirm", len(sifted))
	if err := s.recv(StateConfirming, MsgConfirm); err != nil {
		return s.fail(err.(*Failure))
	}
	if s.cfg.ForceConfirmFailure {
		return s.fail(NewFailure(FailureConfirmation, StateConfirming,
			"peer did not acknowledge key confirmation within retries", nil))
	}
	if err := s.recv(StateConfirming, MsgConfirmAck); err != nil {
		return s.fail(err.(*Failure))
	}

	// 7) established
	key := make([]byte, (len(sifted)+7)/8)
	for i, b := range sifted {
		key[i/8] |= b.alice << uint(i%8)
	}
	s.transition(StateEstablished, "key established bytes=%d", len(key))
	return &Result{
		SessionID: s.id,
		State:     StateEstablished,
		QBER:      qber,
		SharedKey: key,
		Events:    s.events,
	}, nil
}
