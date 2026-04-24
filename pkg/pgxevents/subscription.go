package pgxevents

import "sync"

// Subscription is a single-table, untyped subscription. Events are read
// from the channel returned by Events; Close unsubscribes and closes the
// channel. Close is idempotent.
type Subscription interface {
	Events() <-chan Event
	Close() error
}

// TypedSubscription is a single-table subscription whose events are
// unmarshalled into T. Otherwise behaves like Subscription.
type TypedSubscription[T any] interface {
	Events() <-chan TypedEvent[T]
	Close() error
}

// subscription is the concrete *Subscription returned by listener.Listen.
type subscription struct {
	table    string
	ch       chan Event
	listener *listener

	closeMu   sync.RWMutex
	closed    bool
	closeOnce sync.Once
}

// Events returns the channel events are delivered on.
func (s *subscription) Events() <-chan Event { return s.ch }

// Close detaches the subscription from the listener and closes its event
// channel. Idempotent.
func (s *subscription) Close() error {
	s.listener.removeSubscription(s)
	s.closeInternal()
	return nil
}

// closeInternal closes the subscription channel exactly once. Safe to
// call concurrently with trySend; the closeMu serialises closure with
// any in-flight send.
func (s *subscription) closeInternal() {
	s.closeOnce.Do(func() {
		s.closeMu.Lock()
		defer s.closeMu.Unlock()
		s.closed = true
		close(s.ch)
	})
}

// trySend delivers ev to the subscriber under the configured overflow
// policy. Returns true if the subscription remains alive (event delivered
// or dropped per policy); false if the subscription should be removed
// (e.g. OverflowDisconnect tripped, or already closed).
//
// trySend is called by the listener's listen-loop goroutine; concurrent
// trySend calls on the same subscription would break the
// drop-oldest-then-write invariant.
func (s *subscription) trySend(ev Event, policy OverflowPolicy, m Metrics, log Logger) (alive bool) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		return false
	}

	// Fast path.
	select {
	case s.ch <- ev:
		return true
	default:
	}

	// Overflow.
	switch policy {
	case OverflowDropOldest:
		select {
		case <-s.ch:
			m.EventDropped(ev.Table, "overflow")
		default:
		}
		select {
		case s.ch <- ev:
			return true
		default:
			// Still full despite drain; abandon.
			m.EventDropped(ev.Table, "overflow")
			return true
		}
	case OverflowDisconnect:
		m.EventDropped(ev.Table, "overflow")
		log.Warn("pgxevents: subscriber overflow, disconnecting", "table", ev.Table)
		return false
	}
	return true
}
