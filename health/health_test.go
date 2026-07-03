package health

import (
	"net/http/httptest"
	"testing"
	"time"
)

func healthzCode(t *testing.T, tr *ReadinessTracker) int {
	t.Helper()
	rec := httptest.NewRecorder()
	tr.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	return rec.Code
}

func TestReadinessRequiresEveryTopic(t *testing.T) {
	tr := NewReadinessTracker([]string{"a", "b"})

	if code := healthzCode(t, tr); code != 503 {
		t.Errorf("before any topic is ready: %d, want 503", code)
	}
	tr.MarkReady(0)
	if code := healthzCode(t, tr); code != 503 {
		t.Errorf("with one of two topics ready: %d, want 503", code)
	}
	tr.MarkReady(1)
	if code := healthzCode(t, tr); code != 200 {
		t.Errorf("with all topics ready: %d, want 200", code)
	}
	tr.MarkNotReady(0)
	if code := healthzCode(t, tr); code != 503 {
		t.Errorf("after a topic drops: %d, want 503", code)
	}
}

func TestSnapshotTracksStateTransitions(t *testing.T) {
	tr := NewReadinessTracker([]string{"app1-topic"})

	before := tr.Snapshot()
	if len(before) != 1 || before[0].Topic != "app1-topic" || before[0].Ready || before[0].Since.IsZero() {
		t.Fatalf("initial snapshot wrong: %+v", before)
	}

	time.Sleep(5 * time.Millisecond)
	tr.MarkReady(0)
	after := tr.Snapshot()
	if !after[0].Ready || !after[0].Since.After(before[0].Since) {
		t.Errorf("transition should update ready and since: %+v -> %+v", before[0], after[0])
	}

	// Re-marking the same state must not reset the transition time.
	tr.MarkReady(0)
	if again := tr.Snapshot(); !again[0].Since.Equal(after[0].Since) {
		t.Error("marking an unchanged state must not move the since timestamp")
	}
}

func TestLiveAlwaysOK(t *testing.T) {
	rec := httptest.NewRecorder()
	Live(rec, httptest.NewRequest("GET", "/livez", nil))
	if rec.Code != 200 {
		t.Errorf("livez returned %d", rec.Code)
	}
}
