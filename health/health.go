package health

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ReadinessTracker tracks per-topic consumer readiness and when each topic
// entered its current state. ServeHTTP returns 200 only when every topic is
// ready.
type ReadinessTracker struct {
	mu     sync.RWMutex
	states []topicState
}

type topicState struct {
	topic string
	ready bool
	since time.Time
}

// TopicStatus is one topic's consumer state, as reported to the status page.
type TopicStatus struct {
	Topic string
	Ready bool
	Since time.Time
}

func NewReadinessTracker(topics []string) *ReadinessTracker {
	now := time.Now()
	states := make([]topicState, len(topics))
	for i, t := range topics {
		states[i] = topicState{topic: t, since: now}
	}
	return &ReadinessTracker{states: states}
}

func (r *ReadinessTracker) MarkReady(i int)    { r.set(i, true) }
func (r *ReadinessTracker) MarkNotReady(i int) { r.set(i, false) }

func (r *ReadinessTracker) set(i int, ready bool) {
	r.mu.Lock()
	if r.states[i].ready != ready {
		r.states[i].ready = ready
		r.states[i].since = time.Now()
	}
	r.mu.Unlock()
}

// Snapshot returns the current state of every topic for the status page.
func (r *ReadinessTracker) Snapshot() []TopicStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TopicStatus, len(r.states))
	for i, s := range r.states {
		out[i] = TopicStatus{Topic: s.topic, Ready: s.ready, Since: s.since}
	}
	return out
}

func (r *ReadinessTracker) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for i := range r.states {
		if !r.states[i].ready {
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

// Component is one row on the /statusz page.
type Component struct {
	Name   string    // e.g. "kafka[app1-topic]"
	OK     bool      // false = degraded, needs attention
	Detail string    // human explanation of the current state
	Since  time.Time // when the component entered this state; zero = omit
}

// NewStatusHandler serves a human-readable overview of every component's
// current state on /statusz. collect is invoked per request so the page is
// always live.
func NewStatusHandler(start time.Time, collect func() []Component) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		comps := collect()
		degraded := 0
		for _, c := range comps {
			if !c.OK {
				degraded++
			}
		}
		overall := "OK"
		if degraded > 0 {
			overall = fmt.Sprintf("DEGRADED (%d of %d components)", degraded, len(comps))
		}

		now := time.Now()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "log-processor-go — %s — uptime %s\n\n",
			overall, now.Sub(start).Round(time.Second))
		for _, c := range comps {
			state := "OK      "
			if !c.OK {
				state = "DEGRADED"
			}
			fmt.Fprintf(w, "%s  %-28s  %s", state, c.Name, c.Detail)
			if !c.Since.IsZero() {
				fmt.Fprintf(w, "  [since %s, %s]",
					c.Since.Format("2006-01-02 15:04:05"), now.Sub(c.Since).Round(time.Second))
			}
			fmt.Fprintln(w)
		}
	})
}
