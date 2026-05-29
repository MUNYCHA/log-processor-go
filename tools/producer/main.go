package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type LogEvent struct {
	ServerName string `json:"serverName"`
	Path       string `json:"path"`
	Topic      string `json:"topic"`
	Timestamp  string `json:"timestamp"`
	Message    string `json:"message"`
}

func main() {
	broker := flag.String("broker", "localhost:9092", "Kafka broker address")
	topic := flag.String("topic", "", "Kafka topic to produce to (required)")
	rate := flag.Int("rate", 10000, "Messages per second")
	server := flag.String("server", "load-test-server", "serverName field in each message")
	flag.Parse()

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: -topic is required")
		flag.Usage()
		os.Exit(1)
	}

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.Flush.Frequency = 10 * time.Millisecond
	cfg.Producer.Flush.MaxMessages = 500
	cfg.Version = sarama.V2_0_0_0

	producer, err := sarama.NewAsyncProducer([]string{*broker}, cfg)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	defer producer.AsyncClose()

	var sent atomic.Int64
	var dropped atomic.Int64

	// drain errors in background
	go func() {
		for range producer.Errors() {
			dropped.Add(1)
		}
	}()

	// stats every second
	go func() {
		prev := int64(0)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		start := time.Now()
		for range ticker.C {
			total := sent.Load()
			drop := dropped.Load()
			persec := total - prev
			prev = total
			fmt.Printf("[%5.0fs] sent=%-10d  rate=%-8d/s  dropped=%d\n",
				time.Since(start).Seconds(), total, persec, drop)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	interval := time.Second / time.Duration(*rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	seq := int64(0)
	messages := buildMessages(*topic, *server)

	fmt.Printf("producing to topic=%s at rate=%d/s  broker=%s\n", *topic, *rate, *broker)
	fmt.Println("press Ctrl+C to stop")

	for {
		select {
		case <-ticker.C:
			msg := messages[seq%int64(len(messages))]
			msg.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
			msg.Message = fmt.Sprintf("load test message seq=%d", seq)

			value, _ := json.Marshal(msg)
			select {
			case producer.Input() <- &sarama.ProducerMessage{
				Topic: *topic,
				Value: sarama.ByteEncoder(value),
			}:
				sent.Add(1)
			default:
				dropped.Add(1)
			}
			seq++

		case <-sigCh:
			total := sent.Load()
			drop := dropped.Load()
			fmt.Printf("\nstopped — total sent=%d dropped=%d\n", total, drop)
			return
		}
	}
}

// pre-build a pool of message templates to avoid allocs on the hot path
func buildMessages(topic, server string) []LogEvent {
	paths := []string{
		"/var/log/app/service.log",
		"/var/log/app/worker.log",
		"/var/log/app/api.log",
		"/var/log/app/db.log",
	}
	msgs := make([]LogEvent, len(paths))
	for i, p := range paths {
		msgs[i] = LogEvent{
			ServerName: server,
			Path:       p,
			Topic:      topic,
		}
	}
	return msgs
}
