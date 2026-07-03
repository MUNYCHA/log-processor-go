package logpkg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// waitFor polls cond until it is true or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestDedupDisablesOnDeleteAndRecoversOnRecreate(t *testing.T) {
	recoverProbeInterval = 50 * time.Millisecond

	dir := t.TempDir()
	file := filepath.Join(dir, "patterns.txt")
	if err := os.WriteFile(file, nil, 0644); err != nil {
		t.Fatal(err)
	}

	s := NewAlertPatternStore(file)
	if s.Disabled() {
		t.Fatal("store should start enabled when the file exists")
	}
	if !s.Add("pattern one") {
		t.Fatal("Add should succeed while the file exists")
	}

	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "Add to fail and disable dedup after file deletion", func() bool {
		return !s.Add("pattern two") || s.Disabled()
	})
	if !s.Disabled() {
		t.Fatal("store should be disabled after the pattern file is deleted")
	}

	if err := os.WriteFile(file, []byte("pattern one\n"), 0644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "dedup to re-enable after file recreation", func() bool {
		return !s.Disabled()
	})

	if !s.IsKnown("pattern one") {
		t.Fatal("recovered store should have reloaded patterns from the recreated file")
	}
	if !s.Add("pattern three") {
		t.Fatal("Add should succeed again after recovery")
	}
}

func TestWatcherSurvivesDirectoryRemovalAndRecreation(t *testing.T) {
	recoverProbeInterval = 50 * time.Millisecond
	dirPollInterval = 50 * time.Millisecond

	base := t.TempDir()
	dir := filepath.Join(base, "patterns")
	file := filepath.Join(dir, "patterns.txt")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, nil, 0644); err != nil {
		t.Fatal(err)
	}

	s := NewAlertPatternStore(file)
	if !s.Add("pattern one") {
		t.Fatal("Add should succeed while the directory exists")
	}

	// Remove the whole directory, not just the file.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "dedup to disable after directory removal", func() bool {
		return !s.Add("pattern two") || s.Disabled()
	})

	// Recreate the directory and file; dedup must come back on its own.
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("pattern one\n"), 0644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "dedup to re-enable after directory recreation", func() bool {
		return !s.Disabled()
	})

	// The watcher must also have re-attached: an external edit to the file
	// should be picked up live. Rewrite until the reload lands, since the
	// watcher restart races with this test.
	content := []byte("pattern one\nexternal pattern\n")
	waitFor(t, "watcher to resume live reload after directory recreation", func() bool {
		if err := os.WriteFile(file, content, 0644); err != nil {
			return false
		}
		return s.IsKnown("external pattern")
	})
}

// TestAddRejectsOversizedPattern: huge patterns are never stored (they would
// poison future loads of the file) but must not disable dedup either — the
// alert is simply always sent.
func TestAddRejectsOversizedPattern(t *testing.T) {
	file := filepath.Join(t.TempDir(), "patterns.txt")
	if err := os.WriteFile(file, nil, 0644); err != nil {
		t.Fatal(err)
	}
	s := NewAlertPatternStore(file)

	huge := strings.Repeat("a", maxPatternLen+1)
	if s.Add(huge) {
		t.Error("oversized pattern must not be stored")
	}
	if s.Disabled() {
		t.Error("oversized pattern must not disable dedup")
	}
	if b, _ := os.ReadFile(file); len(b) != 0 {
		t.Error("oversized pattern must not be written to the file")
	}
	if !s.Add("normal pattern") {
		t.Error("normal patterns must still store fine afterwards")
	}
}

// TestPatternCapBoundsMemory: at the cap, existing patterns keep deduping,
// new ones are not stored (their alerts always send), and dedup stays enabled.
func TestPatternCapBoundsMemory(t *testing.T) {
	origCap := maxPatterns
	maxPatterns = 2
	t.Cleanup(func() { maxPatterns = origCap })

	file := filepath.Join(t.TempDir(), "patterns.txt")
	if err := os.WriteFile(file, nil, 0644); err != nil {
		t.Fatal(err)
	}
	s := NewAlertPatternStore(file)

	if !s.Add("pattern one") || !s.Add("pattern two") {
		t.Fatal("adds below the cap must succeed")
	}
	if s.Add("pattern three") {
		t.Error("add at the cap must be refused")
	}
	if s.Disabled() {
		t.Error("hitting the cap must not disable dedup")
	}
	if !s.IsKnown("pattern one") || !s.IsKnown("pattern two") {
		t.Error("existing patterns must keep deduping at the cap")
	}
}

// TestLoadTolerantOfBadLines: one oversized line in the file must not break
// loading — the good patterns still load and dedup stays enabled.
func TestLoadTolerantOfBadLines(t *testing.T) {
	file := filepath.Join(t.TempDir(), "patterns.txt")
	content := "good pattern\n" + strings.Repeat("x", maxPatternLen*2) + "\nanother good one\n"
	if err := os.WriteFile(file, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	s := NewAlertPatternStore(file)

	if s.Disabled() {
		t.Fatal("store must load despite an oversized line")
	}
	if !s.IsKnown("good pattern") || !s.IsKnown("another good one") {
		t.Error("good patterns around the bad line must load")
	}
}

func TestStoreStartsDisabledWhenFileMissingThenRecovers(t *testing.T) {
	recoverProbeInterval = 50 * time.Millisecond

	dir := t.TempDir()
	file := filepath.Join(dir, "patterns.txt")

	s := NewAlertPatternStore(file)
	if !s.Disabled() {
		t.Fatal("store should start disabled when the file is missing")
	}

	if err := os.WriteFile(file, []byte("known pattern\n"), 0644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "dedup to enable once the file appears", func() bool {
		return !s.Disabled()
	})

	if !s.IsKnown("known pattern") {
		t.Fatal("store should have loaded patterns from the newly created file")
	}
}
