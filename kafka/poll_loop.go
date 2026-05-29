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

			// drain any immediately available messages into the same batch
			for {
				drained := false
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
					drained = true
				}
				if drained {
					break
				}
			}

			p.flushAndProcess(session, &buf, batch)
			batch = batch[:0]

		case <-session.Context().Done():
			return nil
		}
	}
}

func (p *PollLoop) flushAndProcess(session sarama.ConsumerGroupSession, buf *strings.Builder, batch []batchItem) {
	if len(batch) == 0 {
		return
	}

	// Phase 1: flush file with retry (block until success or session ends)
	for {
		if err := p.ctx.Writer.Flush(buf.String()); err != nil {
			slog.Error("file flush failed, retrying", "topic", p.ctx.Topic, "error", err)
			select {
			case <-time.After(time.Second):
				continue
			case <-session.Context().Done():
				return
			}
		}
		break
	}
	buf.Reset()

	// Phase 2: alert detection + commit
	var alerts []*logpkg.LogEvent
	for _, item := range batch {
		if alert := p.ctx.Handler.ProcessAlert(item.event); alert != nil {
			alerts = append(alerts, alert)
		}
		session.MarkMessage(item.msg, "")
	}
	session.Commit()

	// Phase 3: enqueue Telegram alerts (non-blocking — drop on full queue)
	for _, alert := range alerts {
		select {
		case p.telegrams <- alert:
		default:
			slog.Error("telegram queue full, alert dropped",
				"topic", alert.Topic, "server", alert.ServerName)
		}
	}
}

// NewSaramaConfig builds the consumer group config matching the Java settings.
func NewSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
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
