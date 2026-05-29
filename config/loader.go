package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const (
	defaultConfigPath = "config/consumer_config.json"
	envKey            = "CONSUMER_CONFIG"
)

func Load(args []string) (*AppConfig, string, error) {
	path := resolvePath(args)

	f, err := os.Open(path)
	if err != nil {
		return nil, path, fmt.Errorf("cannot open config %s: %w", path, err)
	}
	defer f.Close()

	var cfg AppConfig
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, path, fmt.Errorf("cannot parse config %s: %w", path, err)
	}
	return &cfg, path, nil
}

func Validate(cfg *AppConfig) error {
	if strings.TrimSpace(cfg.BootstrapServers) == "" {
		return fmt.Errorf("bootstrapServers must not be empty")
	}
	if strings.TrimSpace(cfg.TelegramBotToken) == "" {
		return fmt.Errorf("telegramBotToken must not be empty")
	}
	if strings.TrimSpace(cfg.TelegramChatId) == "" {
		return fmt.Errorf("telegramChatId must not be empty")
	}
	if len(cfg.Topics) == 0 {
		return fmt.Errorf("topics must not be empty")
	}
	for i, t := range cfg.Topics {
		if strings.TrimSpace(t.Topic) == "" {
			return fmt.Errorf("topics[%d].topic must not be empty", i)
		}
		if strings.TrimSpace(t.Output) == "" {
			return fmt.Errorf("topics[%d].output must not be empty", i)
		}
	}
	return nil
}

func resolvePath(args []string) string {
	for i, arg := range args {
		if strings.HasPrefix(arg, "--config=") {
			return strings.TrimPrefix(arg, "--config=")
		}
		if arg == "--config" && i+1 < len(args) {
			return args[i+1]
		}
		if !strings.HasPrefix(arg, "-") && strings.TrimSpace(arg) != "" {
			return arg
		}
	}
	if v := os.Getenv(envKey); strings.TrimSpace(v) != "" {
		return v
	}
	return defaultConfigPath
}
