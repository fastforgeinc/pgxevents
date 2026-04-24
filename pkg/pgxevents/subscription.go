package pgxevents

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
