package main

import (
	"errors"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"log-processor-go/health"
	"log-processor-go/logpkg"
	"log-processor-go/notification"
	"log-processor-go/pipeline"
)

// TestDefaultMemoryLimit: the binary applies its own soft memory limit when
// run bare (no GOMEMLIMIT in the environment) and defers to the operator's
// value when one is set.
func TestDefaultMemoryLimit(t *testing.T) {
	orig := debug.SetMemoryLimit(-1) // read current without changing
	debug.SetMemoryLimit(orig)
	t.Cleanup(func() { debug.SetMemoryLimit(orig) })

	t.Setenv("GOMEMLIMIT", "")
	debug.SetMemoryLimit(math.MaxInt64)
	applyDefaultMemoryLimit()
	if got := debug.SetMemoryLimit(-1); got != defaultMemoryLimit {
		t.Errorf("bare run limit = %d, want built-in %d", got, int64(defaultMemoryLimit))
	}

	t.Setenv("GOMEMLIMIT", "256MiB")
	debug.SetMemoryLimit(math.MaxInt64)
	applyDefaultMemoryLimit()
	if got := debug.SetMemoryLimit(-1); got != math.MaxInt64 {
		t.Error("an operator-set GOMEMLIMIT must not be overridden")
	}
}

// TestStatuszReportsHealthyAndDegradedComponents renders /statusz with one
// component per state: Kafka disconnected, output writing fine, Telegram
// disabled, and dedup disabled because its pattern file is missing.
func TestStatuszReportsHealthyAndDegradedComponents(t *testing.T) {
	dir := t.TempDir()

	out := filepath.Join(dir, "out.log")
	if err := os.WriteFile(out, nil, 0644); err != nil {
		t.Fatal(err)
	}
	writer := pipeline.NewBatchFileWriter(out)
	if err := writer.Flush("hello\n"); err != nil {
		t.Fatal(err)
	}

	store := logpkg.NewAlertPatternStore(filepath.Join(dir, "missing", "patterns.txt"))

	notifier := notification.NewTelegramNotificationService("token", "chat")
	notifier.Disable(errors.New("dial tcp: connection refused"))

	tracker := health.NewReadinessTracker([]string{"app1-topic"}) // never marked ready

	refs := []topicStatusRefs{{topic: "app1-topic", writer: writer, store: store}}
	mux := newHealthMux(tracker, health.NewStatusHandler(time.Now(), func() []health.Component {
		return collectStatus(tracker, notifier, refs)
	}))

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest("GET", "/statusz", nil))
	body := rec.Body.String()
	t.Log("\n" + body)

	for _, want := range []string{
		"DEGRADED (3 of 4 components)",
		"telegram",
		"alert sending OFF — dial tcp: connection refused",
		"kafka[app1-topic]",
		"disconnected — reconnecting with backoff",
		"output[app1-topic]",
		"writing — last write",
		"dedup[app1-topic]",
		"OFF — initial load failed",
		"recreate the pattern file to re-enable",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("statusz output missing %q\n--- body ---\n%s", want, body)
		}
	}
}
