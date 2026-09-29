package qkd

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func baseCfg(seed int64) SessionConfig {
	return SessionConfig{
		Version:       ProtocolVersion1,
		PhotonCount:   2000,
		QBERThreshold: 0.15,
		Link:          LinkConfig{LossRate: 0.2, NoiseRate: 0.05, Seed: seed},
		MessageTTL:    5_000_000_000,
		Seed:          seed,
		Log:           testLogger{t: nil},
	}
}

func runSession(t *testing.T, mutate func(*SessionConfig)) (*Result, error) {
	t.Helper()
	cfg := baseCfg(7)
	cfg.Log = testLogger{t: t}
	if mutate != nil {
		mutate(&cfg)
	}
	s := NewSession("sess-test", cfg)
	res, err := s.Run(context.Background())
	for _, ev := range res.Events {
		t.Logf("event %s @ %s | %s", ev.State, ev.At.Format("15:04:05.000"), ev.Note)
	}
	return res, err
}

func wantFailure(t *testing.T, err error, code FailureCode, step State) *Failure {
	t.Helper()
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("want *Failure code=%s, got %T: %v", code, err, err)
	}
	if f.Code != code {
		t.Fatalf("want failure code=%s, got=%s (detail=%s)", code, f.Code, f.Detail)
	}
	if f.Step != step {
		t.Fatalf("want failure step=%s, got=%s", step, f.Step)
	}
	return f
}

func TestNegotiationHappyPath(t *testing.T) {
	res, err := runSession(t, nil)
	if err != nil {
		t.Fatalf("expected established, got %v", err)
	}
	if res.State != StateEstablished || len(res.SharedKey) == 0 {
		t.Fatalf("want established with key, state=%s keyLen=%d", res.State, len(res.SharedKey))
	}
	if res.QBER > 0.15 {
		t.Fatalf("qber must be under threshold, got %.4f", res.QBER)
	}
	t.Logf("SUCCESS qber=%.4f keyBytes=%d", res.QBER, len(res.SharedKey))
}

func TestHighQBERAbortsAtEstimating(t *testing.T) {
	_, err := runSession(t, func(c *SessionConfig) {
		c.Link.NoiseRate = 0.6 // 噪声远高于 0.15 阈值
	})
	f := wantFailure(t, err, FailureQBERTooHigh, StateEstimating)
	t.Logf("aborted as expected: %s", f.Error())
}

func TestConfirmFailureAbortsAtConfirming(t *testing.T) {
	_, err := runSession(t, func(c *SessionConfig) {
		c.ForceConfirmFailure = true
	})
	wantFailure(t, err, FailureConfirmation, StateConfirming)
}

func TestPeerAbortIsDistinguishable(t *testing.T) {
	// 对端在 sifting 阶段中止
	_, err := runSession(t, func(c *SessionConfig) {
		c.PeerAbortAfter = StateSifting
	})
	wantFailure(t, err, FailurePeerAborted, StateSifting)
}

func TestVersionIncompatibleAbortsAtHello(t *testing.T) {
	_, err := runSession(t, func(c *SessionConfig) {
		c.PeerVersion = "QKD/0.9"
	})
	wantFailure(t, err, FailureVersionIncompat, StateHello)
}

func TestTotalLossYieldsNoKeyAgreed(t *testing.T) {
	_, err := runSession(t, func(c *SessionConfig) {
		c.Link.LossRate = 1
	})
	wantFailure(t, err, FailureNoKeyAgreed, StateSifting)
}

func TestPublicFailureHidesInternalCause(t *testing.T) {
	cfg := baseCfg(1)
	cfg.Log = testLogger{t: t}
	s := NewSession("sess-pub", cfg)
	inner := errors.New("db connection string leaked secret")
	f := NewFailure(FailureInternal, StateConfirming, "sensitive internal detail xyz", inner)
	pub := f.Public()
	if pub.cause != nil {
		t.Fatalf("public failure must strip internal cause, got %v", pub.cause)
	}
	if strings.Contains(pub.Error(), "secret") || strings.Contains(pub.Error(), "xyz") {
		t.Fatalf("public failure leaked internals: %s", pub.Error())
	}
	// 业务失败码保留，方便调用方区分。
	if pub.Code != FailureInternal || pub.Step != StateConfirming {
		t.Fatalf("code/step must be preserved, got %s/%s", pub.Code, pub.Step)
	}
	_ = s
}
