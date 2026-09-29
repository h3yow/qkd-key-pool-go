// Package link provides a fully local, deterministic-seeded simulation of the
// quantum and classical links between two QKD nodes. Loss, optical disturbance
// (which drives the bit error rate) and detector noise are all configurable.
//
// The simulation never touches a real network or quantum device: frames are
// handed between two in-process endpoints, and randomness comes from a
// reproducible PRNG seeded by Config.Seed.
package link

import (
	"context"
	"math/rand/v2"
	"sync"

	"github.com/h3yow/qkd-key-pool-go/proto"
	"github.com/h3yow/qkd-key-pool-go/qkderr"
)

// Endpoint identifies one side of the point-to-point link.
type Endpoint string

const (
	EndpointA Endpoint = "A"
	EndpointB Endpoint = "B"
)

// PeerOf returns the other endpoint.
func PeerOf(e Endpoint) Endpoint {
	if e == EndpointA {
		return EndpointB
	}
	return EndpointA
}

// Config tunes the simulated links. All rate fields are clamped to [0,1].
type Config struct {
	// LossRate is the per-attempt classical-frame drop probability.
	LossRate float64
	// Disturbance is the channel disturbance/eavesdrop rate; it inflates QBER.
	Disturbance float64
	// DetectorNoise is the detector dark-count/noise rate.
	DetectorNoise float64
	// BaseQBER is the intrinsic error floor even with zero disturbance/noise.
	BaseQBER float64
	// Seed makes the randomized behaviour reproducible. 0 = fixed default seed.
	Seed int64
	// MaxAttempts bounds retransmission over the lossy classical link.
	MaxAttempts int
}

func (c *Config) applyDefaults() {
	clamp := func(x float64) float64 {
		if x < 0 {
			return 0
		}
		if x > 1 {
			return 1
		}
		return x
	}
	c.LossRate = clamp(c.LossRate)
	c.Disturbance = clamp(c.Disturbance)
	c.DetectorNoise = clamp(c.DetectorNoise)
	if c.BaseQBER < 0 {
		c.BaseQBER = 0
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 4
	}
}

// Frame is a classical message in transit, with its source/destination.
type Frame struct {
	From Endpoint
	To   Endpoint
	Msg  proto.Message
}

// Link is the local simulated channel between two endpoints.
type Link struct {
	cfg Config
	mu  sync.Mutex
	rng *rand.Rand

	qMu    sync.Mutex
	queues map[Endpoint][]Frame
	cond   map[Endpoint]*sync.Cond
}

// New constructs a Link with defaults applied for unset fields.
func New(cfg Config) *Link {
	cfg.applyDefaults()
	l := &Link{
		cfg:    cfg,
		rng:    rand.New(rand.NewPCG(uint64(cfg.Seed)+1, uint64(cfg.Seed)^0x9e3779b97f4a7c15)),
		queues: map[Endpoint][]Frame{},
	}
	l.cond = map[Endpoint]*sync.Cond{
		EndpointA: sync.NewCond(&l.qMu),
		EndpointB: sync.NewCond(&l.qMu),
	}
	return l
}

// Config returns the normalized configuration.
func (l *Link) Config() Config { return l.cfg }

// QBER returns the measured quantum bit error rate for one exchange:
//
//	qber = base + disturbance + 0.3*detectorNoise + small jitter
//
// clamped to [0,1]. Disturbance dominates, so a configured eavesdrop/disturb
// level reliably pushes the run over the session's abort threshold.
func (l *Link) QBER() float64 {
	l.mu.Lock()
	jitter := l.rng.Float64() * 0.002 // ±0.2% random measurement noise
	l.mu.Unlock()
	q := l.cfg.BaseQBER + l.cfg.Disturbance + 0.3*l.cfg.DetectorNoise + jitter
	if q < 0 {
		return 0
	}
	if q > 1 {
		return 1
	}
	return q
}

// Deliver sends one classical frame from->to, retrying up to MaxAttempts
// against the configured loss rate. On success the frame waits in the
// destination inbox until Recv. Returns a classified KindDropped error when
// every attempt is lost, or KindMalformed for a structurally invalid message.
func (l *Link) Deliver(ctx context.Context, from, to Endpoint, msg proto.Message) error {
	if err := msg.Validate(); err != nil {
		return err
	}
	if from != EndpointA && from != EndpointB {
		return qkderr.New(qkderr.KindMalformed, "unknown source endpoint")
	}
	if to != EndpointA && to != EndpointB {
		return qkderr.New(qkderr.KindMalformed, "unknown destination endpoint")
	}
	if from == to {
		return qkderr.New(qkderr.KindMalformed, "source and destination must differ")
	}

	for attempt := 1; attempt <= l.cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return qkderr.Wrap(qkderr.KindTimeout, "context cancelled during delivery", err)
		}
		l.mu.Lock()
		lost := l.rng.Float64() < l.cfg.LossRate
		l.mu.Unlock()
		if !lost {
			l.qMu.Lock()
			l.queues[to] = append(l.queues[to], Frame{From: from, To: to, Msg: msg})
			l.cond[to].Broadcast()
			l.qMu.Unlock()
			return nil
		}
	}
	return qkderr.New(qkderr.KindDropped,
		"frame lost after max delivery attempts")
}

// Recv blocks until a frame addressed to 'to' arrives, ctx is done, or the
// link is closed. A cancelled ctx maps to a classified KindTimeout error.
func (l *Link) Recv(ctx context.Context, to Endpoint) (Frame, error) {
	if to != EndpointA && to != EndpointB {
		return Frame{}, qkderr.New(qkderr.KindMalformed, "unknown endpoint")
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			l.qMu.Lock()
			l.cond[to].Broadcast()
			l.qMu.Unlock()
		case <-done:
		}
	}()

	l.qMu.Lock()
	for len(l.queues[to]) == 0 {
		if err := ctx.Err(); err != nil {
			l.qMu.Unlock()
			return Frame{}, qkderr.Wrap(qkderr.KindTimeout, "timed out waiting for frame", err)
		}
		l.cond[to].Wait()
		if err := ctx.Err(); err != nil {
			l.qMu.Unlock()
			return Frame{}, qkderr.Wrap(qkderr.KindTimeout, "timed out waiting for frame", err)
		}
	}
	f := l.queues[to][0]
	l.queues[to] = l.queues[to][1:]
	l.qMu.Unlock()
	return f, nil
}

// QueueLen reports how many frames are buffered for an endpoint.
func (l *Link) QueueLen(to Endpoint) int {
	l.qMu.Lock()
	defer l.qMu.Unlock()
	return len(l.queues[to])
}

// Reset drops all queued frames. Used between independent test runs.
func (l *Link) Reset() {
	l.qMu.Lock()
	l.queues[EndpointA] = nil
	l.queues[EndpointB] = nil
	l.cond[EndpointA].Broadcast()
	l.cond[EndpointB].Broadcast()
	l.qMu.Unlock()
}
