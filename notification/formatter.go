package notification

import (
	"fmt"
	"time"

	"log-processor-go/logpkg"
)

// TelegramAlertFormatter renders a LogEvent into a human-readable Telegram
// alert body. The event timestamp is an ISO-8601 UTC instant; it is
// reformatted in the local system timezone for display.
type TelegramAlertFormatter struct {
	loc *time.Location
}

func NewTelegramAlertFormatter() *TelegramAlertFormatter {
	return &TelegramAlertFormatter{loc: time.Local}
}

func (f *TelegramAlertFormatter) Format(event *logpkg.LogEvent) string {
	ts, err := time.Parse(time.RFC3339Nano, event.Timestamp)
	if err != nil {
		ts, _ = time.Parse(time.RFC3339, event.Timestamp)
	}
	when := ts.In(f.loc).Format("2006-01-02 15:04:05")

	return fmt.Sprintf("ALERT\nTime: %s\nHost: %s\nFile: %s\nTopic: %s\nMessage: %s",
		when, event.ServerName, event.Path, event.Topic, event.Message)
}
