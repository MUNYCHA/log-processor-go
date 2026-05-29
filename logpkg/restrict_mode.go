package logpkg

import "strings"

type RestrictMode int

const (
	High   RestrictMode = iota
	Medium RestrictMode = iota
	Low    RestrictMode = iota
)

func ParseRestrictMode(s string) RestrictMode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "low":
		return Low
	case "medium", "med":
		return Medium
	default:
		return High
	}
}
