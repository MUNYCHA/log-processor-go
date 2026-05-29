package logpkg

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	warnThreshold      = 10_000
	maxWatcherRestarts = 5
	dirPollInterval    = 5 * time.Second
	dirPollMaxAttempts = 24 // 24 × 5s = 2 minutes
)

type AlertPatternStore struct {
	patternFile string
	mu          sync.RWMutex
	known       map[string]struct{}
}

func NewAlertPatternStore(patternFile string) (*AlertPatternStore, error) {
	s := &AlertPatternStore{
		patternFile: patternFile,
		known:       make(map[string]struct{}),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	go s.startWatcher()
	return s, nil
}

func (s *AlertPatternStore) load() error {
	f, err := os.Open(s.patternFile)
	if err != nil {
		return err
	}
	defer f.Close()

	next := make(map[string]struct{})
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			next[line] = struct{}{}
		}
	}
	s.mu.Lock()
	s.known = next
	s.mu.Unlock()
	slog.Info("loaded patterns", "count", len(next), "file", s.patternFile)
	return sc.Err()
}

func (s *AlertPatternStore) reload() {
	f, err := os.Open(s.patternFile)
	if err != nil {
		slog.Warn("reload failed, keeping previous patterns", "error", err)
		return
	}
	defer f.Close()

	next := make(map[string]struct{})
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			next[line] = struct{}{}
		}
	}
	if sc.Err() != nil {
		slog.Warn("reload scan error, keeping previous patterns", "error", sc.Err())
		return
	}

	s.mu.Lock()
	same := mapsEqual(s.known, next)
	if !same {
		s.known = next
	}
	s.mu.Unlock()

	if !same {
		slog.Info("reloaded patterns", "count", len(next), "file", s.patternFile)
	} else {
		slog.Debug("pattern file event contained no external change", "file", s.patternFile)
	}
}

func (s *AlertPatternStore) IsKnown(pattern string) bool {
	s.mu.RLock()
	_, ok := s.known[pattern]
	s.mu.RUnlock()
	return ok
}

// Add persists the pattern to disk before exposing it in-memory.
// Returns false if disk persistence fails; the caller must not send
// a Telegram alert in that case.
func (s *AlertPatternStore) Add(pattern string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.patternFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		slog.Error("failed to open pattern file for append", "file", s.patternFile, "error", err)
		return false
	}
	_, writeErr := f.WriteString(pattern + "\n")
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		slog.Error("failed to persist alert pattern", "file", s.patternFile, "write", writeErr, "close", closeErr)
		return false
	}

	s.known[pattern] = struct{}{}

	if len(s.known) >= warnThreshold {
		slog.Warn("patterns accumulated — normalization may be missing a variable token type",
			"count", len(s.known), "file", s.patternFile)
	}
	return true
}

func (s *AlertPatternStore) startWatcher() {
	crashCount := 0
	for crashCount <= maxWatcherRestarts {
		err := s.runWatchLoop()
		if err == nil {
			return // clean stop
		}
		dir := filepath.Dir(s.patternFile)
		if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
			slog.Warn("watched directory gone, waiting for it to return", "dir", dir)
			if s.waitForDirectory(dir) {
				slog.Info("directory returned, restarting watcher", "dir", dir)
				continue // don't count this as a crash
			}
			slog.Error("FATAL: watched directory did not return after 2 minutes — pattern resets require app restart")
			return
		}
		crashCount++
		if crashCount > maxWatcherRestarts {
			slog.Error("FATAL: watcher stopped after max restarts — pattern resets require app restart",
				"restarts", maxWatcherRestarts, "error", err)
			return
		}
		slog.Warn("watcher crashed, restarting",
			"attempt", crashCount, "max", maxWatcherRestarts, "error", err)
		time.Sleep(2 * time.Second)
	}
}

func (s *AlertPatternStore) runWatchLoop() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	dir := filepath.Dir(s.patternFile)
	if err := watcher.Add(dir); err != nil {
		return err
	}
	filename := filepath.Base(s.patternFile)

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if filepath.Base(event.Name) != filename {
				continue
			}
			if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				s.mu.Lock()
				s.known = make(map[string]struct{})
				s.mu.Unlock()
				slog.Info("pattern file deleted — cleared all patterns")
			} else if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				s.reload()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			return err
		}
	}
}

func (s *AlertPatternStore) waitForDirectory(dir string) bool {
	for i := 0; i < dirPollMaxAttempts; i++ {
		time.Sleep(dirPollInterval)
		if _, err := os.Stat(dir); err == nil {
			return true
		}
		slog.Warn("still waiting for directory to return",
			"attempt", i+1, "max", dirPollMaxAttempts, "dir", dir)
	}
	return false
}

func mapsEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}
