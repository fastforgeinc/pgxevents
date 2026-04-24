package pgxevents

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Listener is the entry point for receiving events from configured tables.
// A single Listener holds one PostgreSQL LISTEN connection internally and
// multiplexes notifications to all active subscriptions.
type Listener interface {
	// Listen returns an untyped Subscription for the given table. Most
	// callers should use ListenTyped instead.
	Listen(table string) (Subscription, error)

	// Health emits a non-nil error when the listener becomes unhealthy
	// (LISTEN connection lost or repeatedly failing to reconnect) and nil
	// when the listener recovers. The channel is closed only by Close.
	//
	// Consumers typically use this to drain their own downstream streams
	// on failure, forcing reconnection-based reconciliation.
	Health() <-chan error

	// Close stops the listener, unsubscribes all subscriptions, drains
	// in-flight deliveries, and releases PostgreSQL resources. Idempotent.
	Close() error
}

// NewListener creates a new Listener backed by the given pgx pool.
//
// By default (MigrationsRequired mode), the listener expects the pgxevents
// DDL — the trigger function, per-table triggers, and outbox table — to
// already be installed via the consumer's own golang-migrate migrations
// using the SQL exported as InstallSQL and TableTriggerSQL. NewListener
// validates at startup that the expected objects exist and match the
// library Version; mismatches fail fast with an actionable error.
//
// Pass WithRuntimeInstall(true) to install DDL at startup instead. This
// is useful in dev/test setups or when golang-migrate is not in use, but
// requires the database user to hold DDL privileges.
func NewListener(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (Listener, error) {
	_ = ctx
	_ = pool
	_ = opts
	panic("pgxevents: NewListener not implemented — v1 scaffold only")
}
