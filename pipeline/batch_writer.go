package pipeline

import (
	"os"
	"sync"
)

// BatchFileWriter appends UTF-8 text to a file. The file is opened and
// closed per flush so log-rotation scripts can truncate or replace the file
// between flushes without the process holding a stale descriptor.
// A mutex serialises concurrent flushes from multiple partition goroutines
// so their content never interleaves in the output file.
type BatchFileWriter struct {
	path string
	mu   sync.Mutex
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
