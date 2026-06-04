package pipeline

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"log-processor-go/logpkg"
)

// LogHandler processes log records in two phases. Output buffering happens
// first; alert work runs only after the output file flush succeeds.
// All fields are read-only after construction — safe for concurrent use
// across multiple ConsumeClaim goroutines (one per partition).
type LogHandler struct {
	detector     *logpkg.AlertDetector
	normalizer   *logpkg.LogMessageNormalizer
	patternStore *logpkg.AlertPatternStore // nil when dedup is disabled
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

	if h.patternStore != nil && !h.patternStore.Disabled() {
		pattern := h.normalizer.NormalizeMessage(event.Message)
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

func parseEvent(value []byte) (*logpkg.LogEvent, error) {
	var event logpkg.LogEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return nil, err
	}
	return &event, nil
}
