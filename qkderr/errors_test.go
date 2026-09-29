package qkderr

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestInternal_DoesNotLeakCause(t *testing.T) {
	secret := errors.New("panic: internals at /secret/path creds=xyz")
	err := Internal(secret)
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "creds") {
		t.Fatalf("internal cause leaked to Error(): %q", err.Error())
	}
	t.Logf("safe Error() = %q", err.Error())
	if KindOf(err) != KindInternal {
		t.Fatalf("kind = %s, want internal", KindOf(err))
	}
	// cause retained for logging via Unwrap, but never rendered.
	if !errors.Is(err, secret) {
		t.Fatalf("Unwrap should retain internal cause for logging")
	}
}

func TestWrap_HidesCauseText(t *testing.T) {
	cause := errors.New("DB connection string postgres://user:pw@host")
	err := Wrap(KindTimeout, "step timed out", cause)
	if strings.Contains(err.Error(), "postgres") {
		t.Fatalf("wrapped cause text leaked: %q", err.Error())
	}
	if KindOf(err) != KindTimeout {
		t.Fatalf("kind = %s, want timeout", KindOf(err))
	}
}

func TestKindStrings_StableAndDistinguishable(t *testing.T) {
	seen := map[string]bool{}
	for k := KindQBERTooHigh; k <= KindInternal; k++ {
		s := k.String()
		if seen[s] {
			t.Fatalf("duplicate kind string %q", s)
		}
		seen[s] = true
	}
	if got := fmt.Sprint(KindUnknown, KindQBERTooHigh, KindConfirmFailed,
		KindPeerAborted, KindTimeout, KindUnsupportedVersion,
		KindDuplicateMessage, KindOutOfOrderMessage, KindStaleMessage,
		KindKeyConsumed, KindKeyExpired, KindLeaseNotHeld); got == "" {
		t.Fatal("expected non-empty kinds")
	}
	t.Logf("all kinds: %v", seen)
}

func TestKindOf_UnknownForPlainError(t *testing.T) {
	if KindOf(errors.New("plain")) != KindUnknown {
		t.Fatal("plain error should map to KindUnknown")
	}
}
