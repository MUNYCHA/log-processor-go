package pipeline

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"log-processor-go/logpkg"
)

const (
	// maxAlertMessageLen caps how much of a message the alert path
	// (normalization regexes, dedup fingerprint, Telegram queue) works on.
	// The output file always receives the full message — this never touches
	// it. Telegram itself cuts messages around 4 KB, and an error's identity
	// lives in its first lines, so nothing of alerting value is lost — while
	// a huge Kafka message (up to ~1 MB) can no longer cost a megabyte of
	// regex scanning per line or pin megabytes per slot in the alert queue.
	maxAlertMessageLen = 8 * 1024

	// normalizeFailLogInterval throttles the normalization-timeout warning so
	// a stream of pathological lines (e.g. a bad custom rule hitting every
	// alert) doesn't spam the log.
	normalizeFailLogInterval = 30 * time.Second
)

// LogHandler processes log records in two phases. Output buffering happens
// first; alert work runs only after the output file flush succeeds.
// All fields are set at construction and safe for concurrent use across
// multiple ConsumeClaim goroutines (one per partition).
type LogHandler struct {
	detector     *logpkg.AlertDetector
	normalizer   *logpkg.LogMessageNormalizer
	patternStore *logpkg.AlertPatternStore // nil when dedup is disabled

	normWarnAt atomic.Int64 // unix nanos of the last normalization-timeout warning
}

func NewLogHandler(
	detector *logpkg.AlertDetector,
	normalizer *logpkg.LogMessageNormalizer,
	patternStore *logpkg.AlertPatternStore,
) *LogHandler {
	return &LogHandler{
		detector:     detector,
		normalizer:   normalizer,
		patternStore: patternStore,
	}
}

// BufferOutput parses the record, appends the message to buf, and returns
// the parsed event so the caller can pass it directly to ProcessAlert after
// a successful flush. Returns an error for invalid records.
func (h *LogHandler) BufferOutput(value []byte, buf *strings.Builder) (*logpkg.LogEvent, error) {
	event, err := parseEvent(value)
	if err != nil {
		return nil, fmt.Errorf("log output preparation failed: %w", err)
	}
	buf.WriteString(event.Message)
	buf.WriteString("\n")
	return event, nil
}

// ProcessAlert runs alert detection and dedup on an already-parsed event.
// Must be called only after the output flush has succeeded.
// Returns the event if a Telegram notification should be sent, nil otherwise.
func (h *LogHandler) ProcessAlert(event *logpkg.LogEvent) *logpkg.LogEvent {
	if !h.detector.Matches(event.Message) {
		return nil
	}

	// Cap the message for everything downstream (the full message is already
	// flushed to the output file). strings.Clone drops the reference to the
	// original large backing array, so a queued event holds at most
	// maxAlertMessageLen bytes instead of pinning the whole Kafka message.
	if len(event.Message) > maxAlertMessageLen {
		event.Message = strings.Clone(truncateUTF8(event.Message, maxAlertMessageLen))
	}

	if h.patternStore != nil && !h.patternStore.Disabled() {
		pattern, err := h.normalizer.NormalizeMessage(event.Message)
		if err != nil {
			// A regex timed out on this line (catastrophic backtracking).
			// Fail open: skip dedup and send the alert rather than suppress.
			h.warnNormalizeFailure(err)
			return event
		}
		if h.patternStore.IsKnown(pattern) {
			slog.Debug("suppressed telegram: pattern already known", "pattern", pattern)
			return nil
		}
		if !h.patternStore.Add(pattern) {
			// Add failed and has disabled the store; send the alert instead of
			// suppressing it. Further alerts skip dedup entirely.
			return event
		}
	}

	return event
}

func (h *LogHandler) warnNormalizeFailure(err error) {
	now := time.Now().UnixNano()
	last := h.normWarnAt.Load()
	if now-last >= int64(normalizeFailLogInterval) && h.normWarnAt.CompareAndSwap(last, now) {
		slog.Warn("normalization timed out — dedup skipped for this line, alert sent", "error", err)
	}
}

// truncateUTF8 cuts s to at most n bytes without splitting a multi-byte rune.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func parseEvent(value []byte) (*logpkg.LogEvent, error) {
	var event logpkg.LogEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return nil, err
	}
	return &event, nil
}
