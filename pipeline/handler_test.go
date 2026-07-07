package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"log-processor-go/logpkg"
)

func record(message string) []byte {
	return []byte(`{"serverName":"srv1","path":"/var/log/app.log","topic":"app1-topic","timestamp":"2026-07-03T10:00:00Z","message":"` + message + `"}`)
}

func newHandler(keywords []string, store *logpkg.AlertPatternStore) *LogHandler {
	return NewLogHandler(
		logpkg.NewAlertDetector(keywords),
		logpkg.NewLogMessageNormalizer(nil, logpkg.High, keywords),
		store,
	)
}

func TestBufferOutputAppendsMessages(t *testing.T) {
	h := newHandler(nil, nil)
	var buf strings.Builder

	ev1, err := h.BufferOutput(record("line one"), &buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.BufferOutput(record("line two"), &buf); err != nil {
		t.Fatal(err)
	}

	if got, want := buf.String(), "line one\nline two\n"; got != want {
		t.Errorf("buffer %q, want %q", got, want)
	}
	if ev1.ServerName != "srv1" || ev1.Topic != "app1-topic" || ev1.Message != "line one" {
		t.Errorf("parsed event fields wrong: %+v", ev1)
	}
}

func TestBufferOutputRejectsInvalidJSON(t *testing.T) {
	h := newHandler(nil, nil)
	var buf strings.Builder
	if _, err := h.BufferOutput([]byte("not json"), &buf); err == nil {
		t.Fatal("invalid JSON must return an error")
	}
	if buf.Len() != 0 {
		t.Error("invalid record must not pollute the output buffer")
	}
}

func TestProcessAlertNoKeywordMatch(t *testing.T) {
	h := newHandler([]string{"FATAL"}, nil)
	ev := &logpkg.LogEvent{Message: "all systems nominal"}
	if h.ProcessAlert(ev) != nil {
		t.Error("non-matching message must not alert")
	}
}

func TestProcessAlertWithoutStoreSendsEveryMatch(t *testing.T) {
	h := newHandler([]string{"FATAL"}, nil)
	for i := 0; i < 2; i++ {
		ev := &logpkg.LogEvent{Message: "FATAL: broken"}
		if h.ProcessAlert(ev) == nil {
			t.Fatal("every matching alert must be sent when dedup is not configured")
		}
	}
}

// TestProcessAlertDedup is the end-to-end dedup contract: two alerts that
// differ only in variable data (IP, duration, count) produce one Telegram
// send and one persisted pattern; a structurally different alert still sends.
func TestProcessAlertDedup(t *testing.T) {
	patternFile := filepath.Join(t.TempDir(), "patterns.txt")
	if err := os.WriteFile(patternFile, nil, 0644); err != nil {
		t.Fatal(err)
	}
	store := logpkg.NewAlertPatternStore(patternFile)
	h := newHandler([]string{"FATAL"}, store)

	first := &logpkg.LogEvent{Message: "FATAL: could not connect to 10.0.0.1:5432 after 30s"}
	if h.ProcessAlert(first) == nil {
		t.Fatal("first alert of a new pattern must be sent")
	}

	dup := &logpkg.LogEvent{Message: "FATAL: could not connect to 192.168.9.7:6543 after 45s"}
	if h.ProcessAlert(dup) != nil {
		t.Error("structurally identical alert must be suppressed")
	}

	other := &logpkg.LogEvent{Message: "FATAL: disk full on /var/lib/data"}
	if h.ProcessAlert(other) == nil {
		t.Error("structurally different alert must be sent")
	}

	b, err := os.ReadFile(patternFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Count(string(b), "\n")
	if lines != 2 {
		t.Errorf("pattern file should hold 2 patterns, has %d:\n%s", lines, b)
	}
}

// TestProcessAlertStoreDisabledSendsEverything: when the pattern file is
// missing the store starts disabled and dedup must fail open — every alert
// is sent rather than silently suppressed.
func TestProcessAlertStoreDisabledSendsEverything(t *testing.T) {
	store := logpkg.NewAlertPatternStore(filepath.Join(t.TempDir(), "missing.txt"))
	h := newHandler([]string{"FATAL"}, store)

	for i := 0; i < 2; i++ {
		ev := &logpkg.LogEvent{Message: "FATAL: same shape every time"}
		if h.ProcessAlert(ev) == nil {
			t.Fatal("alerts must be sent while dedup is disabled")
		}
	}
}

// TestProcessAlertTruncatesHugeMessages: the alert path must cap the message
// so a huge Kafka record can't pin megabytes in the Telegram queue or feed
// megabytes through the normalizer regexes. The cut must not split a rune.
func TestProcessAlertTruncatesHugeMessages(t *testing.T) {
	h := newHandler([]string{"FATAL"}, nil)

	// A multi-byte rune straddling the cut boundary must be dropped whole.
	big := "FATAL: payload " + strings.Repeat("é", maxAlertMessageLen)
	ev := &logpkg.LogEvent{Message: big}
	out := h.ProcessAlert(ev)
	if out == nil {
		t.Fatal("oversized alert must still be sent")
	}
	if len(out.Message) > maxAlertMessageLen {
		t.Errorf("queued message is %d bytes, cap is %d", len(out.Message), maxAlertMessageLen)
	}
	if !strings.HasPrefix(out.Message, "FATAL: payload ") {
		t.Errorf("truncation must keep the head of the message, got %q…", out.Message[:32])
	}
	for _, r := range out.Message {
		if r == '�' {
			t.Fatal("truncation split a multi-byte rune")
		}
	}

	small := &logpkg.LogEvent{Message: "FATAL: short"}
	if got := h.ProcessAlert(small); got == nil || got.Message != "FATAL: short" {
		t.Error("messages under the cap must pass through untouched")
	}
}

// TestProcessAlertNormalizationTimeoutFailsOpen: when a regex times out on a
// pathological line (here forced via a catastrophic custom rule), the alert
// must be sent — every time — rather than suppressed, and nothing may be
// stored in the pattern file.
func TestProcessAlertNormalizationTimeoutFailsOpen(t *testing.T) {
	patternFile := filepath.Join(t.TempDir(), "patterns.txt")
	if err := os.WriteFile(patternFile, nil, 0644); err != nil {
		t.Fatal(err)
	}
	store := logpkg.NewAlertPatternStore(patternFile)
	rules := []logpkg.NormalizerRule{logpkg.NewNormalizerRule(`(a+)+b`, "<X>")}
	h := NewLogHandler(
		logpkg.NewAlertDetector([]string{"FATAL"}),
		logpkg.NewLogMessageNormalizer(rules, logpkg.High, nil),
		store,
	)

	ev := &logpkg.LogEvent{Message: "FATAL " + strings.Repeat("a", 64) + "c"}
	for i := 0; i < 2; i++ {
		if h.ProcessAlert(ev) == nil {
			t.Fatal("alert must be sent when normalization times out (fail open)")
		}
	}
	if h.patternStore.Disabled() {
		t.Error("a normalization timeout must not disable the store")
	}
	if b, _ := os.ReadFile(patternFile); len(b) != 0 {
		t.Errorf("no pattern may be stored for a timed-out line, file has %q", b)
	}
}
