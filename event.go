package pgxevents

import "time"

// Event is the untyped form of a mutation event, delivered by Subscription.
// Data is the row snapshot serialized as JSON at transaction-commit time.
// For ActionDelete, the snapshot reflects the row's pre-delete state.
type Event struct {
	Table  string
	Action Action
	Data   []byte
	Meta   Meta
}

// TypedEvent is the typed form, delivered by TypedSubscription[T]. Row is
// populated by unmarshalling the snapshot JSON into T. For ActionDelete,
// Row reflects the row's pre-delete state.
type TypedEvent[T any] struct {
	Table  string
	Action Action
	Row    *T
	Meta   Meta
}

// Meta carries delivery metadata sourced from the outbox row. OutboxID is
// the UUID of the pgxevents_outbox row; consumers may use it for
// idempotency keys or tracing. CreatedAt is the outbox row's timestamp,
// i.e. the commit time of the mutating transaction.
type Meta struct {
	OutboxID  string
	CreatedAt time.Time
}
