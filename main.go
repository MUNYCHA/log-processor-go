package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"log-processor-go/config"
	"log-processor-go/kafka"
	"log-processor-go/logpkg"
	"log-processor-go/notification"
	"log-processor-go/pipeline"
)

const (
	consumerGroupID        = "file-log-consumer"
	telegramQueueCapacity  = 1000
	consumerShutdownWait   = 15 * time.Second
	telegramDrainWait      = 30 * time.Second
)

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

	telegramCh := make(chan *logpkg.LogEvent, telegramQueueCapacity)

	saramaConfig := kafka.NewSaramaConfig()

	var consumerWg sync.WaitGroup
	type consumerHandle struct {
		cg     sarama.ConsumerGroup
		cancel context.CancelFunc
	}
	handles := make([]consumerHandle, 0, len(cfg.Topics))

	for i := range cfg.Topics {
		t := &cfg.Topics[i]

		handler, err := buildHandler(t)
		if err != nil {
			slog.Error("failed to build handler", "topic", t.Topic, "error", err)
			os.Exit(1)
		}

		writer := pipeline.NewBatchFileWriter(t.Output)
		topicCtx := &kafka.TopicContext{Topic: t.Topic, Handler: handler, Writer: writer}
		loop := kafka.NewPollLoop(topicCtx, formatter, telegramCh)

		cg, err := sarama.NewConsumerGroup([]string{cfg.BootstrapServers}, consumerGroupID, saramaConfig)
		if err != nil {
			slog.Error("failed to create consumer group", "topic", t.Topic, "error", err)
			os.Exit(1)
		}

		ctx, cancel := context.WithCancel(context.Background())
		handles = append(handles, consumerHandle{cg: cg, cancel: cancel})

		topic := t.Topic
		consumerWg.Add(1)
		go func() {
			defer consumerWg.Done()
			defer cg.Close()
			for {
				if err := cg.Consume(ctx, []string{topic}, loop); err != nil {
					slog.Error("consumer group error", "topic", topic, "error", err)
					return
				}
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}

	// Single goroutine drives all Telegram sends (serialised, rate-limited).
	var telegramWg sync.WaitGroup
	telegramWg.Add(1)
	go func() {
		defer telegramWg.Done()
		for event := range telegramCh {
			notifier.Send(formatter.Format(event))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	slog.Info("shutdown signal received, stopping poll loops...")

	for _, h := range handles {
		h.cancel()
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

	slog.Info("shutdown complete")
}

func buildHandler(t *config.TopicConfig) (*pipeline.LogHandler, error) {
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
		var err error
		store, err = logpkg.NewAlertPatternStore(t.PatternStoreFile)
		if err != nil {
			return nil, err
		}
	}

	return pipeline.NewLogHandler(detector, normalizer, store), nil
}

func validatePaths(t *config.TopicConfig) error {
	if err := checkWritableFile(t.Output); err != nil {
		return fmt.Errorf("output file: %w", err)
	}
	if t.HasPatternStore() {
		if err := checkWritableFile(t.PatternStoreFile); err != nil {
			return fmt.Errorf("pattern store file: %w", err)
		}
	}
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
