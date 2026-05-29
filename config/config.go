package config

import "strings"

type AppConfig struct {
	BootstrapServers string        `json:"bootstrapServers"`
	TelegramBotToken string        `json:"telegramBotToken"`
	TelegramChatId   string        `json:"telegramChatId"`
	Topics           []TopicConfig `json:"topics"`
}

type TopicConfig struct {
	Topic                      string              `json:"topic"`
	Output                     string              `json:"output"`
	AlertKeywords              []string            `json:"alertKeywords"`
	PatternStoreFile           string              `json:"patternStoreFile"`
	CustomNormalizationRules   []NormalizationRule `json:"customNormalizationRules"`
	PatternExtractRestrictMode string              `json:"patternExtractRestrictMode"`
}

func (t *TopicConfig) HasAlertKeywords() bool {
	return len(t.AlertKeywords) > 0
}

func (t *TopicConfig) HasPatternStore() bool {
	return strings.TrimSpace(t.PatternStoreFile) != ""
}

func (t *TopicConfig) HasCustomNormalizationRules() bool {
	return len(t.CustomNormalizationRules) > 0
}

type NormalizationRule struct {
	Pattern     string `json:"pattern"`
	Replacement string `json:"replacement"`
}
