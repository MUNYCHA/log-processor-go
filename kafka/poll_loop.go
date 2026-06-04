package kafka

import (
	"log/slog"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"log-processor-go/logpkg"
	"log-processor-go/notification"
	"log-processor-go/pipeline"
)

// TopicContext is the per-topic wiring handed to the poll loop.
type TopicContext struct {
	Topic   string
	Handler *pipeline.LogHandler
	Writer  *pipeline.BatchFileWriter
}

// PollLoop implements sarama.ConsumerGroupHandler for a single topic.
// It is safe for concurrent use — sarama calls ConsumeClaim in a separate
// goroutine for each assigned partition.
type PollLoop struct {
	ctx       *TopicContext
	formatter *notification.TelegramAlertFormatter
	telegrams chan<- *logpkg.LogEvent
}

func NewPollLoop(
	ctx *TopicContext,
	formatter *notification.TelegramAlertFormatter,
	telegrams chan<- *logpkg.LogEvent,
) *PollLoop {
	return &PollLoop{ctx: ctx, formatter: formatter, telegrams: telegrams}
}

func (p *PollLoop) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (p *PollLoop) Cleanup(sarama.ConsumerGroupSession) error { return nil }

const (
	// maxBatchMessages / maxBatchBytes cap a single flush batch so a backlog
	// burst can't build an unbounded-size batch in memory. Whichever is hit
	// first ends the drain and triggers a flush.
	maxBatchMessages = 500
	maxBatchBytes    = 1 << 20 // 1 MB
	// bufShrinkCap: if the output buffer grows past this, drop it and start
	// fresh after a flush instead of Reset() (which retains peak capacity).
	bufShrinkCap = 2 * maxBatchBytes
	// fileFailLogInterval throttles the "file not writable" reminder so a
	// sustained output outage doesn't spam the log every second.
	fileFailLogInterval = 30 * time.Second
)

// batchItem pairs a raw Kafka message with its already-parsed event so
// ProcessAlert never needs to re-parse the JSON.
type batchItem struct {
	msg   *sarama.ConsumerMessage
	event *logpkg.LogEvent
}

func (p *PollLoop) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var buf strings.Builder
	var batch []batchItem

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			event, err := p.ctx.Handler.BufferOutput(msg.Value, &buf)
			if err != nil {
				slog.Error("invalid record skipped",
					"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
				continue
			}
			batch = append(batch, batchItem{msg, event})

			// Drain immediately-available messages into the same batch, but stop
			// once the batch hits its size cap so a backlog can't build an
			// unbounded batch in memory.
		drain:
			for len(batch) < maxBatchMessages && buf.Len() < maxBatchBytes {
				select {
				case m, ok := <-claim.Messages():
					if !ok {
						p.flushAndProcess(session, &buf, batch)
						return nil
					}
					ev, err := p.ctx.Handler.BufferOutput(m.Value, &buf)
					if err != nil {
						slog.Error("invalid record skipped",
							"topic", m.Topic, "partition", m.Partition, "offset", m.Offset, "error", err)
					} else {
						batch = append(batch, batchItem{m, ev})
					}
				default:
					break drain
				}
			}

			if !p.flushAndProcess(session, &buf, batch) {
				return nil
			}

			// Release retained memory so a one-time burst can't pin a
			// high-water-mark floor for the life of this partition goroutine.
			for i := range batch {
				batch[i] = batchItem{}
			}
			batch = batch[:0]
			if buf.Cap() > bufShrinkCap {
				buf = strings.Builder{}
			} else {
				buf.Reset()
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

// flushAndProcess writes the batch to the output file (blocking-retry until it
// succeeds or the session ends), then runs alert detection and enqueues
// Telegram alerts. It returns false if the session ended before the flush could
// succeed, signalling the caller to stop. Offsets are never committed: on
// reconnect the consumer starts from the newest offset, so committing would be
// meaningless.
func (p *PollLoop) flushAndProcess(session sarama.ConsumerGroupSession, buf *strings.Builder, batch []batchItem) bool {
	if len(batch) == 0 {
		return true
	}

	// Phase 1: flush file.
	if !p.flushWithRetry(session, buf.String()) {
		return false
	}

	// Phase 2: alert detection.
	var alerts []*logpkg.LogEvent
	for _, item := range batch {
		if alert := p.ctx.Handler.ProcessAlert(item.event); alert != nil {
			alerts = append(alerts, alert)
		}
	}

	// Phase 3: enqueue Telegram alerts (non-blocking — drop on full queue).
	for _, alert := range alerts {
		select {
		case p.telegrams <- alert:
		default:
			slog.Error("telegram queue full, alert dropped",
				"topic", alert.Topic, "server", alert.ServerName)
		}
	}
	return true
}

// flushWithRetry blocks writing the content until the output file accepts it or
// the session ends. While the file is unwritable it pauses (no data loss) and
// logs once on the first failure, then only occasionally, plus once on recovery.
// Returns false if the session ended before the write succeeded.
func (p *PollLoop) flushWithRetry(session sarama.ConsumerGroupSession, content string) bool {
	var failStart, lastLog time.Time
	for {
		if err := p.ctx.Writer.Flush(content); err == nil {
			if !failStart.IsZero() {
				slog.Info("output file writable again, resuming writes",
					"topic", p.ctx.Topic, "paused_for", time.Since(failStart).Round(time.Second))
			}
			return true
		} else {
			now := time.Now()
			switch {
			case failStart.IsZero():
				failStart, lastLog = now, now
				slog.Error("output file not writable, pausing writes (will retry quietly)",
					"topic", p.ctx.Topic, "error", err)
			case now.Sub(lastLog) >= fileFailLogInterval:
				lastLog = now
				slog.Error("output file still not writable",
					"topic", p.ctx.Topic, "paused_for", time.Since(failStart).Round(time.Second), "error", err)
			}
		}
		select {
		case <-time.After(time.Second):
		case <-session.Context().Done():
			return false
		}
	}
}

// NewSaramaConfig builds the consumer group config matching the Java settings.
func NewSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Fetch.Max = 1_048_576  // 1 MB per fetch response
	cfg.Consumer.Fetch.Default = 262_144 // 256 KB per partition
	cfg.ChannelBufferSize = 200
	cfg.Consumer.Group.Session.Timeout = 10 * time.Second
	cfg.Consumer.Group.Rebalance.Timeout = 300 * time.Second
	cfg.Version = sarama.V2_0_0_0
	return cfg
}
