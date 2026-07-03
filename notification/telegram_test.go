package notification

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// flakyTelegram is a fake Telegram API whose failure mode can be flipped at
// runtime: in failing mode it kills the TCP connection without a response
// (looks like a network outage to the client); otherwise it answers 200.
type flakyTelegram struct {
	failing  atomic.Bool
	lastText atomic.Value // string: last sendMessage text received
}

func (f *flakyTelegram) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if f.failing.Load() {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		if strings.HasSuffix(r.URL.Path, "/sendMessage") {
			var body struct {
				Text string `json:"text"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.lastText.Store(body.Text)
		}
		w.WriteHeader(http.StatusOK)
	}
}

func newTestService(t *testing.T, h http.Handler) *TelegramNotificationService {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	svc := NewTelegramNotificationService("test-token", "chat-1")
	svc.apiBase = srv.URL
	return svc
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestSendDeliversMessage(t *testing.T) {
	fake := &flakyTelegram{}
	svc := newTestService(t, fake.handler())

	if err := svc.Probe(); err != nil {
		t.Fatalf("probe against healthy server failed: %v", err)
	}
	svc.Send("FATAL: disk full")

	if svc.Disabled() {
		t.Error("service must stay enabled after a successful send")
	}
	if got, _ := fake.lastText.Load().(string); got != "FATAL: disk full" {
		t.Errorf("server received %q, want the alert text", got)
	}
}

// TestSendDisablesOnOutageThenRecovers is the fix-1 contract: a connectivity
// outage disables sending after retries, and the background probe re-enables
// it on its own once Telegram answers again — no restart.
func TestSendDisablesOnOutageThenRecovers(t *testing.T) {
	recoverProbeInterval = 50 * time.Millisecond
	fake := &flakyTelegram{}
	fake.failing.Store(true)
	svc := newTestService(t, fake.handler())

	svc.Send("dropped during outage") // ~2s: retries then disables

	if !svc.Disabled() {
		t.Fatal("service must disable after a persistent connectivity failure")
	}
	ok, since, reason := svc.Status()
	if ok || since.IsZero() || reason == "" {
		t.Errorf("Status should describe the outage: ok=%v since=%v reason=%q", ok, since, reason)
	}

	fake.failing.Store(false)
	waitFor(t, "background probe to re-enable sending", func() bool {
		return !svc.Disabled()
	})

	svc.Send("after recovery")
	if got, _ := fake.lastText.Load().(string); got != "after recovery" {
		t.Errorf("send after recovery delivered %q", got)
	}
}

// TestPersistentRateLimitDropsAlertButStaysEnabled: HTTP 429 means Telegram
// is reachable, just throttled — the alert is dropped but alerting must not
// disable itself.
func TestPersistentRateLimitDropsAlertButStaysEnabled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"parameters":{"retry_after":1}}`))
	}))
	t.Cleanup(srv.Close)
	svc := NewTelegramNotificationService("test-token", "chat-1")
	svc.apiBase = srv.URL

	svc.Send("throttled alert") // ~3s: three 1s retry-after waits

	if svc.Disabled() {
		t.Error("persistent 429 must not disable alerting")
	}
}
