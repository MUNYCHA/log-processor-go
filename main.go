package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"log-processor-go/config"
	"log-processor-go/health"
	"log-processor-go/kafka"
	"log-processor-go/logpkg"
	"log-processor-go/notification"
	"log-processor-go/pipeline"
)

const (
	telegramQueueCapacity    = 1000
	consumerShutdownWait     = 15 * time.Second
	telegramDrainWait        = 30 * time.Second
	telegramDisabledReminder = 5 * time.Minute
	healthAddr               = ":8080"
)

// consumerGroupID is unique per process start so the consumer always begins at
// the newest offset (no committed offsets to resume from) on both the initial
// connect and every reconnect. This skips any backlog that accumulated during a
// Kafka outage instead of replaying it — keeping memory flat through recovery.
var consumerGroupID = fmt.Sprintf("file-log-consumer-%d", time.Now().UnixNano())

func main() {
	cfg, path, err := config.Load(os.Args[1:])
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	slog.Info("using config", "path", path)

	if err := config.Validate(cfg); err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}

	for i := range cfg.Topics {
		if err := validatePaths(&cfg.Topics[i]); err != nil {
			slog.Error("path validation failed", "error", err)
			os.Exit(1)
		}
	}

	formatter := notification.NewTelegramAlertFormatter()
	notifier := notification.NewTelegramNotificationService(cfg.TelegramBotToken, cfg.TelegramChatId)

	// Telegram is best-effort: if it is unreachable at startup, disable alert
	// sending for the rest of the run rather than blocking on every send.
	if err := notifier.Probe(); err != nil {
		notifier.Disable(err)
	}

	telegramCh := make(chan *logpkg.LogEvent, telegramQueueCapacity)

	saramaConfig := kafka.NewSaramaConfig()

	tracker := health.NewReadinessTracker(len(cfg.Topics))

	healthSrv := &http.Server{
		Addr:    healthAddr,
		Handler: newHealthMux(tracker),
	}
	go func() {
		slog.Info("health server listening", "addr", healthAddr)
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server error", "error", err)
		}
	}()

	var consumerWg sync.WaitGroup
	cancels := make([]context.CancelFunc, 0, len(cfg.Topics))

	for i := range cfg.Topics {
		t := &cfg.Topics[i]

		handler := buildHandler(t)

		writer := pipeline.NewBatchFileWriter(t.Output)
		topicCtx := &kafka.TopicContext{Topic: t.Topic, Handler: handler, Writer: writer}
		loop := kafka.NewPollLoop(topicCtx, formatter, telegramCh)

		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)

		brokers := cfg.BootstrapServers
		topic := t.Topic
		idx := i

		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			runConsumer(ctx, brokers, topic, loop, saramaConfig, tracker, idx)
		}()
	}

	// Single goroutine drives all Telegram sends (serialised, rate-limited).
	var telegramWg sync.WaitGroup
	telegramWg.Add(1)
	go func() {
		defer telegramWg.Done()
		var lastReminder time.Time
		for event := range telegramCh {
			if notifier.Disabled() {
				// Drain and discard so the queue can't back up; remind only
				// occasionally that alerting is off.
				if time.Since(lastReminder) >= telegramDisabledReminder {
					lastReminder = time.Now()
					slog.Warn("telegram disabled — dropping alerts for this run")
				}
				continue
			}
			notifier.Send(formatter.Format(event))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	slog.Info("shutdown signal received, stopping poll loops...")

	for _, cancel := range cancels {
		cancel()
	}

	done := make(chan struct{})
	go func() { consumerWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(consumerShutdownWait):
		slog.Warn("poll loops did not stop in time")
	}

	slog.Info("draining pending Telegram alerts...")
	close(telegramCh)

	tDone := make(chan struct{})
	go func() { telegramWg.Wait(); close(tDone) }()
	select {
	case <-tDone:
	case <-time.After(telegramDrainWait):
		slog.Warn("Telegram sender did not drain in time")
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = healthSrv.Shutdown(shutCtx)

	slog.Info("shutdown complete")
}

// runConsumer owns the full consumer group lifecycle for one topic.
// It retries the initial connection and any mid-run disconnects using
// exponential backoff, marking the health tracker accordingly.
func runConsumer(
	ctx context.Context,
	brokers, topic string,
	loop sarama.ConsumerGroupHandler,
	cfg *sarama.Config,
	tracker *health.ReadinessTracker,
	idx int,
) {
	cg, err := kafka.NewConsumerGroupWithRetry(ctx, []string{brokers}, consumerGroupID, cfg)
	if err != nil {
		return // ctx cancelled during startup retry
	}
	tracker.MarkReady(idx)
	slog.Info("connected to kafka", "topic", topic)
	go logConsumerErrors(ctx, cg, topic)

	for {
		if err := cg.Consume(ctx, []string{topic}, loop); err != nil {
			slog.Error("consumer error, reconnecting", "topic", topic, "error", err)
			tracker.MarkNotReady(idx)
			cg.Close()

			cg, err = kafka.NewConsumerGroupWithRetry(ctx, []string{brokers}, consumerGroupID, cfg)
			if err != nil {
				return // ctx cancelled during reconnect retry
			}
			tracker.MarkReady(idx)
			slog.Info("reconnected to kafka", "topic", topic)
			go logConsumerErrors(ctx, cg, topic)
			continue
		}
		if ctx.Err() != nil {
			cg.Close()
			return
		}
	}
}

// logConsumerErrors drains a consumer group's error channel so transient
// broker/partition errors are visible. The channel is closed by cg.Close(),
// which ends this goroutine; ctx guards against leaks during shutdown.
func logConsumerErrors(ctx context.Context, cg sarama.ConsumerGroup, topic string) {
	for {
		select {
		case err, ok := <-cg.Errors():
			if !ok {
				return
			}
			slog.Error("consumer group error", "topic", topic, "error", err)
		case <-ctx.Done():
			return
		}
	}
}

func newHealthMux(tracker *health.ReadinessTracker) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/healthz", tracker)
	mux.HandleFunc("/livez", health.Live)
	return mux
}

func buildHandler(t *config.TopicConfig) *pipeline.LogHandler {
	var rules []logpkg.NormalizerRule
	if t.HasCustomNormalizationRules() {
		for _, r := range t.CustomNormalizationRules {
			if r.Pattern == "" || r.Replacement == "" {
				continue
			}
			rules = append(rules, logpkg.NewNormalizerRule(r.Pattern, r.Replacement))
		}
	}

	mode := logpkg.ParseRestrictMode(t.PatternExtractRestrictMode)

	var keepWords []string
	if t.HasAlertKeywords() {
		keepWords = t.AlertKeywords
	}

	normalizer := logpkg.NewLogMessageNormalizer(rules, mode, keepWords)
	detector := logpkg.NewAlertDetector(t.AlertKeywords)

	var store *logpkg.AlertPatternStore
	if t.HasPatternStore() {
		s, err := logpkg.NewAlertPatternStore(t.PatternStoreFile)
		if err != nil {
			// Pattern dedup is best-effort: if the store can't be loaded, run
			// without it (every detected alert is sent) rather than refusing to
			// start. The file is never created by the app.
			slog.Error("pattern store unavailable — dedup disabled for this topic, all alerts will be sent",
				"topic", t.Topic, "file", t.PatternStoreFile, "error", err)
		} else {
			store = s
		}
	}

	return pipeline.NewLogHandler(detector, normalizer, store)
}

func validatePaths(t *config.TopicConfig) error {
	if err := checkWritableFile(t.Output); err != nil {
		return fmt.Errorf("output file: %w", err)
	}
	// The pattern store file is intentionally not validated here: if it is
	// missing or unwritable, the store disables dedup at runtime (see
	// buildHandler / AlertPatternStore) instead of blocking startup.
	return nil
}

func checkWritableFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s: not a regular file", path)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("%s: not writable: %w", path, err)
	}
	f.Close()
	return nil
}
