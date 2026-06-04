package notification

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// TelegramNotificationService sends messages to a Telegram chat.
// Send is not safe for concurrent use — it is intended to be driven by a single
// goroutine. The disabled flag is atomic so it can be read from elsewhere.
type TelegramNotificationService struct {
	botToken        string
	chatId          string
	lastSend        time.Time
	minSendInterval time.Duration
	client          *http.Client
	disabled        atomic.Bool
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

// Disabled reports whether alert sending has been turned off for this run.
func (t *TelegramNotificationService) Disabled() bool { return t.disabled.Load() }

// Disable turns off alert sending for the rest of the run, logging once on the
// transition. Subsequent calls are no-ops. Used when Telegram is unreachable at
// startup or a live send fails on a connectivity error.
func (t *TelegramNotificationService) Disable(err error) {
	if t.disabled.CompareAndSwap(false, true) {
		slog.Error("telegram unreachable — alert sending disabled for this run", "error", err)
	}
}

// Probe checks Telegram reachability once (via getMe). Returns an error if the
// endpoint cannot be reached or rejects the token.
func (t *TelegramNotificationService) Probe() error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/getMe", t.botToken)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram getMe returned %d", resp.StatusCode)
	}
	return nil
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
			// Reachable but rate-limited: back off and retry; don't disable.
			t.minSendInterval = min(t.minSendInterval*2, maxSendInterval)
			time.Sleep(time.Duration(retryAfter) * time.Second)
			continue
		}

		// Connectivity failure (timeout / refused / DNS). Retry a couple times,
		// then treat Telegram as down and disable for the rest of the run.
		if i < maxRetries {
			time.Sleep(time.Second)
			continue
		}
		t.Disable(err)
		return
	}

	// Loop exhausted on persistent 429s: Telegram is reachable, just throttled —
	// drop this alert but keep alerting enabled.
	slog.Warn("telegram persistently rate-limited, alert dropped")
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
