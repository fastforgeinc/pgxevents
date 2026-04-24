package pgxevents

import (
	"sync"
	"testing"
)

// recordingMetrics is a Metrics that captures EventDropped reasons.
type recordingMetrics struct {
	NoopMetrics
	mu      sync.Mutex
	dropped []string
}

func (m *recordingMetrics) EventDropped(_ string, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropped = append(m.dropped, reason)
}

func (m *recordingMetrics) droppedReasons() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.dropped))
	copy(out, m.dropped)
	return out
}

func TestSubscriptionFastPath(t *testing.T) {
	sub := &subscription{table: "t", ch: make(chan Event, 4)}
	m := &recordingMetrics{}

	for _, b := range []string{"1", "2"} {
		if !sub.trySend(Event{Table: "t", Data: []byte(b)}, OverflowDropOldest, m, NoopLogger{}) {
			t.Fatalf("trySend returned false on fast path with byte %q", b)
		}
	}

	got := []string{string((<-sub.ch).Data), string((<-sub.ch).Data)}
	want := []string{"1", "2"}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("ev[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if reasons := m.droppedReasons(); len(reasons) != 0 {
		t.Errorf("expected no drops on fast path, got %v", reasons)
	}
}

func TestSubscriptionDropOldest(t *testing.T) {
	sub := &subscription{table: "t", ch: make(chan Event, 2)}
	m := &recordingMetrics{}

	for _, b := range []string{"1", "2", "3"} {
		if !sub.trySend(Event{Table: "t", Data: []byte(b)}, OverflowDropOldest, m, NoopLogger{}) {
			t.Fatalf("trySend returned false on drop-oldest with byte %q", b)
		}
	}

	got := []string{string((<-sub.ch).Data), string((<-sub.ch).Data)}
	if got[0] != "2" || got[1] != "3" {
		t.Errorf("after drop-oldest got %v, want [2 3]", got)
	}
	reasons := m.droppedReasons()
	if len(reasons) != 1 || reasons[0] != "overflow" {
		t.Errorf("expected one overflow drop, got %v", reasons)
	}
}

func TestSubscriptionDisconnect(t *testing.T) {
	sub := &subscription{table: "t", ch: make(chan Event, 2)}
	m := &recordingMetrics{}

	for _, b := range []string{"1", "2"} {
		if !sub.trySend(Event{Table: "t", Data: []byte(b)}, OverflowDisconnect, m, NoopLogger{}) {
			t.Fatalf("trySend returned false during fill with byte %q", b)
		}
	}

	if alive := sub.trySend(Event{Table: "t"}, OverflowDisconnect, m, NoopLogger{}); alive {
		t.Error("trySend should return false (not alive) on disconnect overflow")
	}
	reasons := m.droppedReasons()
	if len(reasons) != 1 || reasons[0] != "overflow" {
		t.Errorf("expected one overflow drop, got %v", reasons)
	}
}

func TestSubscriptionClosedReturnsNotAlive(t *testing.T) {
	sub := &subscription{table: "t", ch: make(chan Event, 1)}
	sub.closeInternal()

	if alive := sub.trySend(Event{Table: "t"}, OverflowDropOldest, &recordingMetrics{}, NoopLogger{}); alive {
		t.Error("trySend on closed subscription should return false")
	}
}

func TestSubscriptionCloseInternalIdempotent(t *testing.T) {
	sub := &subscription{table: "t", ch: make(chan Event, 1)}
	sub.closeInternal()
	sub.closeInternal() // must not panic on double close
}
