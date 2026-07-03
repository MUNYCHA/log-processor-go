package logpkg

import "testing"

func TestAlertDetectorMatches(t *testing.T) {
	d := NewAlertDetector([]string{"FATAL", " deadlock ", "", "could not"})

	cases := []struct {
		name, msg string
		want      bool
	}{
		{"exact keyword", "FATAL: out of memory", true},
		{"case-insensitive message", "process died: fatal signal", true},
		{"case-insensitive keyword", "Deadlock detected on table x", true},
		{"keyword inside word", "deadlocked transactions: 3", true},
		{"multi-word keyword", "ERROR: could not connect", true},
		{"no match", "all systems nominal", false},
		{"empty message", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := d.Matches(c.msg); got != c.want {
				t.Errorf("Matches(%q) = %v, want %v", c.msg, got, c.want)
			}
		})
	}
}

func TestAlertDetectorNoKeywordsNeverMatches(t *testing.T) {
	for _, d := range []*AlertDetector{
		NewAlertDetector(nil),
		NewAlertDetector([]string{"", "  "}),
	} {
		if d.Matches("FATAL error") {
			t.Error("detector without keywords must never match")
		}
	}
}
