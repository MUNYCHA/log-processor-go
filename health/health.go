package health

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// ReadinessTracker tracks per-topic consumer readiness.
// Each topic slot is independently marked ready/not-ready.
// ServeHTTP returns 200 only when every slot is ready.
type ReadinessTracker struct {
	states []atomic.Bool
}

func NewReadinessTracker(n int) *ReadinessTracker {
	return &ReadinessTracker{states: make([]atomic.Bool, n)}
}

func (r *ReadinessTracker) MarkReady(i int)    { r.states[i].Store(true) }
func (r *ReadinessTracker) MarkNotReady(i int) { r.states[i].Store(false) }

func (r *ReadinessTracker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	for i := range r.states {
		if !r.states[i].Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
	}
	fmt.Fprintln(w, "ok")
}

// Live is a liveness handler: it returns 200 as long as the process is
// running. Unlike readiness, it does not depend on Kafka connectivity, so a
// supervisor won't kill a healthy process that is merely waiting to reconnect.
func Live(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprintln(w, "alive")
}
