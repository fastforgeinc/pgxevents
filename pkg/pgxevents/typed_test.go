package pgxevents

import (
	"testing"
	"time"
)

// stubSubscription is a Subscription implementation for white-box testing
// of typedSubscription. The owner of the stub is responsible for closing
// the events channel; Close on the stub closes it.
type stubSubscription struct {
	ch chan Event
}

func (s *stubSubscription) Events() <-chan Event { return s.ch }
func (s *stubSubscription) Close() error         { close(s.ch); return nil }

type policyRow struct {
	Name string `json:"name"`
	Seat int    `json:"seat"`
}

func TestTypedSubscriptionDecodeAndForward(t *testing.T) {
	raw := &stubSubscription{ch: make(chan Event, 4)}
	metrics := &recordingMetrics{}
	ts := &typedSubscription[policyRow]{
		raw:     raw,
		ch:      make(chan TypedEvent[policyRow], 4),
		logger:  NoopLogger{},
		metrics: metrics,
		table:   "policy_data",
	}
	go ts.run()

	raw.ch <- Event{Table: "policy_data", Action: ActionInsert, Data: []byte(`{"name":"alice","seat":3}`)}
	raw.ch <- Event{Table: "policy_data", Action: ActionUpdate, Data: []byte(`not json`)}
	raw.ch <- Event{Table: "policy_data", Action: ActionDelete, Data: []byte(`{"name":"bob","seat":7}`)}

	got1 := receiveTyped(t, ts.Events())
	if got1.Action != ActionInsert || got1.Row.Name != "alice" || got1.Row.Seat != 3 {
		t.Errorf("got %+v, want Insert/alice/3", got1)
	}
	got2 := receiveTyped(t, ts.Events())
	if got2.Action != ActionDelete || got2.Row.Name != "bob" || got2.Row.Seat != 7 {
		t.Errorf("got %+v, want Delete/bob/7", got2)
	}

	reasons := metrics.droppedReasons()
	if len(reasons) != 1 || reasons[0] != "decode_error" {
		t.Errorf("expected one decode_error, got %v", reasons)
	}

	// Closing the raw subscription closes the typed channel.
	_ = raw.Close()
	select {
	case _, ok := <-ts.Events():
		if ok {
			t.Error("typed channel still open after raw close")
		}
	case <-time.After(time.Second):
		t.Fatal("typed channel did not close within 1s of raw close")
	}
}

func receiveTyped[T any](t *testing.T, ch <-chan TypedEvent[T]) TypedEvent[T] {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for typed event")
		return TypedEvent[T]{}
	}
}
