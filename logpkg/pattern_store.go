package logpkg

import (
	"bufio"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	warnThreshold           = 10_000
	watcherRestartBaseDelay = 2 * time.Second
	watcherRestartMaxDelay  = 60 * time.Second
	dirWaitLogInterval      = time.Minute
)

// recoverProbeInterval is how often a disabled store re-checks whether the
// pattern file is back. dirPollInterval is how often a vanished parent
// directory is re-checked. Vars (not consts) so tests can shorten them.
var (
	recoverProbeInterval = 30 * time.Second
	dirPollInterval      = 5 * time.Second
)

type AlertPatternStore struct {
	patternFile string
	mu          sync.RWMutex
	known       map[string]struct{}
	disabled    atomic.Bool

	// stateMu guards the status-page fields below. It is separate from mu
	// because disable() runs while Add() already holds mu.
	stateMu       sync.Mutex
	stateSince    time.Time // when the enabled/disabled state last changed
	disableReason string    // why dedup is off; "" when enabled
}

// Disabled reports whether dedup is currently off (because the pattern file is
// missing or unwritable). When disabled, callers should send every alert
// rather than suppress. Dedup re-enables itself once the file is back — see
// recoverLoop.
func (s *AlertPatternStore) Disabled() bool { return s.disabled.Load() }

// disable turns dedup off, logging once on the transition, and starts a
// background loop that re-enables it as soon as the pattern file is readable
// and writable again. The file is never created by the app.
func (s *AlertPatternStore) disable(reason string, err error) {
	if s.disabled.CompareAndSwap(false, true) {
		s.setState(reason + ": " + err.Error())
		slog.Error("pattern file unavailable — dedup disabled, all alerts will be sent until the file is back",
			"file", s.patternFile, "reason", reason, "error", err)
		go s.recoverLoop()
	}
}

func (s *AlertPatternStore) setState(reason string) {
	s.stateMu.Lock()
	s.stateSince = time.Now()
	s.disableReason = reason
	s.stateMu.Unlock()
}

// Status reports the store's current state for the status page.
func (s *AlertPatternStore) Status() (ok bool, since time.Time, reason string, patterns int) {
	ok = !s.disabled.Load()
	s.stateMu.Lock()
	since, reason = s.stateSince, s.disableReason
	s.stateMu.Unlock()
	s.mu.RLock()
	patterns = len(s.known)
	s.mu.RUnlock()
	return ok, since, reason, patterns
}

// recoverLoop probes the pattern file until it is writable and loadable again,
// then re-enables dedup with a freshly loaded pattern set. Runs once per
// disable transition (guarded by the CompareAndSwap in disable) and exits on
// success. Probe failures are silent to keep a long outage from spamming logs.
func (s *AlertPatternStore) recoverLoop() {
	for {
		time.Sleep(recoverProbeInterval)
		f, err := os.OpenFile(s.patternFile, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			continue
		}
		f.Close()
		if err := s.load(); err != nil {
			continue
		}
		s.setState("")
		s.disabled.Store(false)
		slog.Info("pattern file available again — dedup re-enabled", "file", s.patternFile)
		return
	}
}

// NewAlertPatternStore always returns a usable store. If the pattern file is
// missing or unreadable at construction, dedup starts disabled and recovers
// automatically once the file appears.
func NewAlertPatternStore(patternFile string) *AlertPatternStore {
	s := &AlertPatternStore{
		patternFile: patternFile,
		known:       make(map[string]struct{}),
		stateSince:  time.Now(),
	}
	if err := s.load(); err != nil {
		s.disable("initial load failed", err)
	}
	go s.startWatcher()
	return s
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

	// No O_CREATE: the app never creates the pattern file. If it is missing or
	// unwritable, dedup disables itself (below) and all alerts are sent.
	f, err := os.OpenFile(s.patternFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		s.disable("open for append failed", err)
		return false
	}
	_, writeErr := f.WriteString(pattern + "\n")
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		s.disable("persist failed", errors.Join(writeErr, closeErr))
		return false
	}

	s.known[pattern] = struct{}{}

	if len(s.known) >= warnThreshold {
		slog.Warn("patterns accumulated — normalization may be missing a variable token type",
			"count", len(s.known), "file", s.patternFile)
	}
	return true
}

// startWatcher keeps the watch loop alive for the life of the process — it
// never gives up. A vanished parent directory is waited out indefinitely
// (dedup re-enabling is handled independently by recoverLoop), and any other
// watcher failure restarts with capped exponential backoff so a persistent
// problem logs at most about once a minute.
func (s *AlertPatternStore) startWatcher() {
	delay := watcherRestartBaseDelay
	for {
		err := s.runWatchLoop()
		if err == nil {
			return // clean stop
		}
		dir := filepath.Dir(s.patternFile)
		if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
			s.waitForDirectory(dir)
			delay = watcherRestartBaseDelay // outage over, not a crash: reset backoff
			continue
		}
		slog.Warn("pattern file watcher failed, restarting", "in", delay, "error", err)
		time.Sleep(delay)
		delay = min(delay*2, watcherRestartMaxDelay)
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
			// When the watched directory itself is removed or renamed, inotify
			// silently drops the watch — without this check the loop would keep
			// running but never see another event. Surface it as an error so
			// startWatcher waits for the directory and re-establishes the watch.
			if event.Name == dir && (event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename)) {
				return errors.New("watched directory removed or renamed")
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

// waitForDirectory blocks until dir exists again — no time limit. It logs once
// when the wait starts, then only an occasional reminder, so a long outage
// cannot flood the log.
func (s *AlertPatternStore) waitForDirectory(dir string) {
	slog.Warn("watched directory gone — waiting for it to return", "dir", dir)
	start := time.Now()
	lastLog := start
	for {
		time.Sleep(dirPollInterval)
		if _, err := os.Stat(dir); err == nil {
			slog.Info("directory returned, restarting pattern file watcher",
				"dir", dir, "gone_for", time.Since(start).Round(time.Second))
			return
		}
		if time.Since(lastLog) >= dirWaitLogInterval {
			lastLog = time.Now()
			slog.Warn("still waiting for watched directory",
				"dir", dir, "gone_for", time.Since(start).Round(time.Second))
		}
	}
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
