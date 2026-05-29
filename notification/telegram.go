package notification

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// TelegramNotificationService sends messages to a Telegram chat.
// Not safe for concurrent use — intended to be driven by a single goroutine.
type TelegramNotificationService struct {
	botToken        string
	chatId          string
	lastSend        time.Time
	minSendInterval time.Duration
	client          *http.Client
}

const (
	baseSendInterval = 3 * time.Second
	maxSendInterval  = 60 * time.Second
	maxRetries       = 3
)

func NewTelegramNotificationService(botToken, chatId string) *TelegramNotificationService {
	return &TelegramNotificationService{
		botToken:        botToken,
		chatId:          chatId,
		minSendInterval: baseSendInterval,
		client: &http.Client{
			Timeout: 7 * time.Second,
		},
	}
}

func (t *TelegramNotificationService) Send(message string) {
	t.enforceRateLimit()

	for i := 1; i <= maxRetries; i++ {
		retryAfter, err := t.sendRequest(message)

		if err == nil {
			t.lastSend = time.Now()
			t.minSendInterval = baseSendInterval
			return
		}

		if retryAfter > 0 {
			t.minSendInterval = min(t.minSendInterval*2, maxSendInterval)
			slog.Warn("telegram 429 rate-limited",
				"retry_after_s", retryAfter, "new_interval_s", t.minSendInterval.Seconds())
			time.Sleep(time.Duration(retryAfter) * time.Second)
			continue
		}

		// timeout
		slog.Warn("telegram timeout", "attempt", i, "max", maxRetries)
		if i == maxRetries {
			slog.Error("telegram FAILED after timeouts, alert dropped", "attempts", maxRetries)
			return
		}
		time.Sleep(time.Second)
	}

	slog.Error("telegram FAILED after max retries (persistent 429), alert dropped", "attempts", maxRetries)
}

func (t *TelegramNotificationService) enforceRateLimit() {
	elapsed := time.Since(t.lastSend)
	if elapsed < t.minSendInterval {
		time.Sleep(t.minSendInterval - elapsed)
	}
}

// sendRequest returns (retryAfter=0, nil) on success,
// (retryAfter>0, err) on 429, (0, err) on timeout or other error.
func (t *TelegramNotificationService) sendRequest(message string) (retryAfter int, err error) {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.botToken)

	body, _ := json.Marshal(map[string]string{
		"chat_id": t.chatId,
		"text":    message,
	})

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")

	resp, err := t.client.Do(req)
	if err != nil {
		return 0, err // includes timeouts
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		io.Copy(io.Discard, resp.Body)
		return 0, nil
	case http.StatusTooManyRequests:
		var result struct {
			Parameters struct {
				RetryAfter int `json:"retry_after"`
			} `json:"parameters"`
		}
		if jsonErr := json.NewDecoder(resp.Body).Decode(&result); jsonErr != nil || result.Parameters.RetryAfter == 0 {
			return 5, fmt.Errorf("429 rate limited")
		}
		return result.Parameters.RetryAfter, fmt.Errorf("429 rate limited")
	case http.StatusBadRequest:
		return 0, fmt.Errorf("bad request (JSON likely malformed)")
	default:
		return 0, fmt.Errorf("telegram HTTP error: %d", resp.StatusCode)
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
