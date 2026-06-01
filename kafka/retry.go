package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

const (
	retryInitialDelay = 2 * time.Second
	retryMaxDelay     = 60 * time.Second
)

// NewConsumerGroupWithRetry creates a consumer group, retrying with exponential
// backoff (2s → 4s → … → 60s cap) until it succeeds or ctx is cancelled.
func NewConsumerGroupWithRetry(ctx context.Context, brokers []string, groupID string, cfg *sarama.Config) (sarama.ConsumerGroup, error) {
	delay := retryInitialDelay
	for {
		cg, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
		if err == nil {
			return cg, nil
		}
		slog.Error("broker unreachable, retrying", "error", err, "in", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		delay = min(delay*2, retryMaxDelay)
	}
}
