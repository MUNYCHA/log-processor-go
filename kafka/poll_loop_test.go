package kafka

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"log-processor-go/logpkg"
	"log-processor-go/notification"
	"log-processor-go/pipeline"
)

// fakeSession implements sarama.ConsumerGroupSession; only Context matters here.
type fakeSession struct{ ctx context.Context }

func (fakeSession) Claims() map[string][]int32                  { return nil }
func (fakeSession) MemberID() string                            { return "test" }
func (fakeSession) GenerationID() int32                         { return 1 }
func (fakeSession) MarkOffset(string, int32, int64, string)     {}
func (fakeSession) Commit()                                     {}
func (fakeSession) ResetOffset(string, int32, int64, string)    {}
func (fakeSession) MarkMessage(*sarama.ConsumerMessage, string) {}
func (s fakeSession) Context() context.Context                  { return s.ctx }

// fakeClaim implements sarama.ConsumerGroupClaim backed by a plain channel.
type fakeClaim struct{ ch chan *sarama.ConsumerMessage }

func (fakeClaim) Topic() string                              { return "app1-topic" }
func (fakeClaim) Partition() int32                           { return 0 }
func (fakeClaim) InitialOffset() int64                       { return 0 }
func (fakeClaim) HighWaterMarkOffset() int64                 { return 0 }
func (c fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.ch }

func msg(payload string) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Topic: "app1-topic", Value: []byte(payload)}
}

func newLoop(t *testing.T, outputPath string, telegrams chan *logpkg.LogEvent) *PollLoop {
	t.Helper()
	handler := pipeline.NewLogHandler(
		logpkg.NewAlertDetector([]string{"FATAL"}),
		logpkg.NewLogMessageNormalizer(nil, logpkg.High, nil),
		nil,
	)
	writer := pipeline.NewBatchFileWriter(outputPath)
	ctx := &TopicContext{Topic: "app1-topic", Handler: handler, Writer: writer}
	return NewPollLoop(ctx, notification.NewTelegramAlertFormatter(), telegrams)
}

// TestConsumeClaimWritesBatchAndEnqueuesAlerts drives the full per-partition
// flow with fakes: valid records land in the output file in order, a corrupt
// record is skipped without stalling, and the alerting record is enqueued
// for Telegram — all before the claim channel closes.
func TestConsumeClaimWritesBatchAndEnqueuesAlerts(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out.log")
	if err := os.WriteFile(out, nil, 0644); err != nil {
		t.Fatal(err)
	}
	telegrams := make(chan *logpkg.LogEvent, 10)
	loop := newLoop(t, out, telegrams)

	ch := make(chan *sarama.ConsumerMessage, 10)
	ch <- msg(`{"serverName":"srv1","topic":"app1-topic","message":"line one"}`)
	ch <- msg(`corrupt not-json record`)
	ch <- msg(`{"serverName":"srv1","topic":"app1-topic","message":"FATAL: boom"}`)
	close(ch)

	if err := loop.ConsumeClaim(fakeSession{ctx: context.Background()}, fakeClaim{ch: ch}); err != nil {
		t.Fatalf("ConsumeClaim returned %v", err)
	}

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), "line one\nFATAL: boom\n"; got != want {
		t.Errorf("output file %q, want %q", got, want)
	}

	select {
	case ev := <-telegrams:
		if ev.Message != "FATAL: boom" {
			t.Errorf("enqueued alert %q, want the FATAL record", ev.Message)
		}
	default:
		t.Error("the FATAL record should have been enqueued for Telegram")
	}
	select {
	case ev := <-telegrams:
		t.Errorf("unexpected extra alert enqueued: %q", ev.Message)
	default:
	}
}

// TestFlushWithRetryWaitsForFileRecreation is the "output file deleted
// mid-run" scenario: the flush pauses, holds the data, and completes as soon
// as the operator recreates the file.
func TestFlushWithRetryWaitsForFileRecreation(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out.log") // never created yet
	loop := newLoop(t, out, make(chan *logpkg.LogEvent, 1))

	done := make(chan bool, 1)
	go func() {
		done <- loop.flushWithRetry(fakeSession{ctx: context.Background()}, "held data\n")
	}()

	time.Sleep(300 * time.Millisecond) // let the first attempt fail
	select {
	case <-done:
		t.Fatal("flush must keep retrying while the file is missing")
	default:
	}

	if err := os.WriteFile(out, nil, 0644); err != nil {
		t.Fatal(err)
	}
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("flush should report success after the file returns")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush did not complete after the file was recreated")
	}

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != "held data\n" {
		t.Errorf("recreated file content %q — buffered data must not be lost", got)
	}
}

// TestFlushWithRetryStopsWhenSessionEnds: shutdown or rebalance must be able
// to interrupt the retry loop instead of hanging forever on a missing file.
func TestFlushWithRetryStopsWhenSessionEnds(t *testing.T) {
	out := filepath.Join(t.TempDir(), "never-exists.log")
	loop := newLoop(t, out, make(chan *logpkg.LogEvent, 1))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		done <- loop.flushWithRetry(fakeSession{ctx: ctx}, "data\n")
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("flush should report failure when the session ends first")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush retry loop did not stop on session end")
	}
}
