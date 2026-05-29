package logpkg

import "strings"

type AlertDetector struct {
	keywords []string
}

func NewAlertDetector(keywords []string) *AlertDetector {
	var normalized []string
	for _, k := range keywords {
		if t := strings.TrimSpace(k); t != "" {
			normalized = append(normalized, strings.ToLower(t))
		}
	}
	return &AlertDetector{keywords: normalized}
}

func (d *AlertDetector) Matches(message string) bool {
	if len(d.keywords) == 0 || message == "" {
		return false
	}
	lower := strings.ToLower(message)
	for _, k := range d.keywords {
		if strings.Contains(lower, k) {
			return true
		}
	}
	return false
}
