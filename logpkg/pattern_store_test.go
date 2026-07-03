package logpkg

import (
	"os"
	"path/filepath"
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
