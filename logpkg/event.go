package logpkg

type LogEvent struct {
	ServerName string `json:"serverName"`
	Path       string `json:"path"`
	Topic      string `json:"topic"`
	Timestamp  string `json:"timestamp"`
	Message    string `json:"message"`
}
