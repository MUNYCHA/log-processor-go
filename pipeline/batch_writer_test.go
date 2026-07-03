package pipeline

import (
	"os"
	"path/filepath"
	"testing"
)

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestFlushAppendsInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")
	if err := os.WriteFile(path, []byte("old\n"), 0644); err != nil {
		t.Fatal(err)
	}
	w := NewBatchFileWriter(path)

	if err := w.Flush("one\n"); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush("two\n"); err != nil {
		t.Fatal(err)
	}
	if got, want := readFile(t, path), "old\none\ntwo\n"; got != want {
		t.Errorf("file content %q, want %q", got, want)
	}

	ok, _, lastWrite, _ := w.Status()
	if !ok || lastWrite.IsZero() {
		t.Errorf("Status after successful flush: ok=%v lastWrite=%v", ok, lastWrite)
	}
}

func TestFlushEmptyContentIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.log")
	w := NewBatchFileWriter(path)
	if err := w.Flush(""); err != nil {
		t.Fatalf("empty flush should never fail, got %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("empty flush must not create the file")
	}
}

// TestFlushNeverCreatesFile pins the core file-safety rule: the app never
// creates the output file, it only appends to one the operator created.
func TestFlushNeverCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.log")
	w := NewBatchFileWriter(path)

	if err := w.Flush("data\n"); err == nil {
		t.Fatal("flush to a missing file must fail")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("failed flush must not create the file")
	}
	ok, failingSince, _, detail := w.Status()
	if ok || failingSince.IsZero() || detail == "" {
		t.Errorf("Status should report the outage: ok=%v since=%v detail=%q", ok, failingSince, detail)
	}
}

// TestFlushRecoversWhenFileRecreated is the operator scenario: output file
// deleted mid-run, then recreated — the next flush must simply succeed.
func TestFlushRecoversWhenFileRecreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatal(err)
	}
	w := NewBatchFileWriter(path)
	if err := w.Flush("before\n"); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush("lost\n"); err == nil {
		t.Fatal("flush after deletion must fail")
	}

	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush("after\n"); err != nil {
		t.Fatalf("flush after recreation should succeed, got %v", err)
	}
	if got, want := readFile(t, path), "after\n"; got != want {
		t.Errorf("file content %q, want %q", got, want)
	}
	if ok, _, _, _ := w.Status(); !ok {
		t.Error("Status should be OK again after recovery")
	}
}

// TestFlushAfterTruncate covers copytruncate-style log rotation: clearing the
// file in place must be transparent — the next flush writes from the start.
func TestFlushAfterTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatal(err)
	}
	w := NewBatchFileWriter(path)
	if err := w.Flush("first\n"); err != nil {
		t.Fatal(err)
	}

	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush("second\n"); err != nil {
		t.Fatal(err)
	}
	if got, want := readFile(t, path), "second\n"; got != want {
		t.Errorf("file content after truncate %q, want %q", got, want)
	}
}
