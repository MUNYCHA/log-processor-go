package pipeline

import (
	"os"
	"sync"
	"time"
)

// BatchFileWriter appends UTF-8 text to a file. The file is opened and
// closed per flush so log-rotation scripts can truncate or replace the file
// between flushes without the process holding a stale descriptor.
// A mutex serialises concurrent flushes from multiple partition goroutines
// so their content never interleaves in the output file.
// It also records the outcome of the most recent flush for the status page.
type BatchFileWriter struct {
	path string
	mu   sync.Mutex

	lastWrite    time.Time // last successful flush; zero if none yet
	failingSince time.Time // first failure of the current outage; zero when OK
	lastErr      string
}

func NewBatchFileWriter(path string) *BatchFileWriter {
	return &BatchFileWriter{path: path}
}

func (w *BatchFileWriter) Flush(content string) error {
	if content == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.write(content); err != nil {
		if w.failingSince.IsZero() {
			w.failingSince = time.Now()
		}
		w.lastErr = err.Error()
		return err
	}
	w.lastWrite = time.Now()
	w.failingSince = time.Time{}
	w.lastErr = ""
	return nil
}

func (w *BatchFileWriter) write(content string) error {
	f, err := os.OpenFile(w.path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	_, writeErr := f.WriteString(content)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// Status reports the writer's last known state for the status page.
// ok is false while flushes are failing; failingSince is the start of the
// current outage. lastWrite is zero if nothing has been written yet.
func (w *BatchFileWriter) Status() (ok bool, failingSince, lastWrite time.Time, errDetail string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failingSince.IsZero(), w.failingSince, w.lastWrite, w.lastErr
}
