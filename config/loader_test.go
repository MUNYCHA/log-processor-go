package config

import (
	"os"
	"path/filepath"
	"testing"
)

const sampleConfig = `{
  "bootstrapServers": "broker:9092",
  "telegramBotToken": "token",
  "telegramChatId": "chat",
  "topics": [
    {"topic": "app1-topic", "output": "/data/logs/app1.log", "alertKeywords": ["FATAL"]}
  ]
}`

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadFromCLIFlag(t *testing.T) {
	path := writeConfig(t, sampleConfig)
	cfg, usedPath, err := Load([]string{"--config=" + path})
	if err != nil {
		t.Fatal(err)
	}
	if usedPath != path {
		t.Errorf("used path %q, want %q", usedPath, path)
	}
	if cfg.BootstrapServers != "broker:9092" || len(cfg.Topics) != 1 || cfg.Topics[0].Topic != "app1-topic" {
		t.Errorf("parsed config wrong: %+v", cfg)
	}
}

func TestLoadFromEnvVar(t *testing.T) {
	path := writeConfig(t, sampleConfig)
	t.Setenv("CONSUMER_CONFIG", path)
	cfg, _, err := Load(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TelegramChatId != "chat" {
		t.Errorf("parsed config wrong: %+v", cfg)
	}
}

func TestLoadErrors(t *testing.T) {
	if _, _, err := Load([]string{"--config=/does/not/exist.json"}); err == nil {
		t.Error("missing file must error")
	}
	bad := writeConfig(t, "{not json")
	if _, _, err := Load([]string{"--config=" + bad}); err == nil {
		t.Error("malformed JSON must error")
	}
}

func TestValidate(t *testing.T) {
	valid := func() *AppConfig {
		return &AppConfig{
			BootstrapServers: "broker:9092",
			TelegramBotToken: "token",
			TelegramChatId:   "chat",
			Topics:           []TopicConfig{{Topic: "t1", Output: "/data/out.log"}},
		}
	}

	if err := Validate(valid()); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*AppConfig)
	}{
		{"empty bootstrapServers", func(c *AppConfig) { c.BootstrapServers = " " }},
		{"empty telegramBotToken", func(c *AppConfig) { c.TelegramBotToken = "" }},
		{"empty telegramChatId", func(c *AppConfig) { c.TelegramChatId = "" }},
		{"no topics", func(c *AppConfig) { c.Topics = nil }},
		{"topic without name", func(c *AppConfig) { c.Topics[0].Topic = "" }},
		{"topic without output", func(c *AppConfig) { c.Topics[0].Output = "  " }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := valid()
			c.mutate(cfg)
			if Validate(cfg) == nil {
				t.Errorf("%s must fail validation", c.name)
			}
		})
	}
}
