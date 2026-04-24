package pgxevents

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrClosed is returned by operations on a closed Listener.
var ErrClosed = errors.New("pgxevents: listener is closed")

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

// listener is the concrete Listener returned by NewListener.
type listener struct {
	pool *pgxpool.Pool
	cfg  config

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	subs   map[string][]*subscription
	closed bool

	healthCh chan error
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
// is useful in dev/test setups, but requires the database user to hold
// DDL privileges.
//
// The returned Listener owns one connection from pool while it is running;
// size the pool accordingly.
func NewListener(parent context.Context, pool *pgxpool.Pool, opts ...Option) (Listener, error) {
	cfg := defaults()
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithCancel(parent)
	l := &listener{
		pool:     pool,
		cfg:      cfg,
		ctx:      ctx,
		cancel:   cancel,
		subs:     make(map[string][]*subscription),
		healthCh: make(chan error, 1),
	}

	if cfg.runtimeInstall {
		if err := runtimeInstallBase(ctx, pool, cfg); err != nil {
			cancel()
			return nil, fmt.Errorf("pgxevents: runtime install: %w", err)
		}
	} else {
		if err := validateInstallBase(ctx, pool); err != nil {
			cancel()
			return nil, err
		}
	}

	l.wg.Add(2)
	go func() { defer l.wg.Done(); l.listenLoop() }()
	go func() { defer l.wg.Done(); l.cleanupLoop() }()

	return l, nil
}

// Listen returns an untyped Subscription for the given table.
func (l *listener) Listen(table string) (Subscription, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil, ErrClosed
	}

	if l.cfg.runtimeInstall {
		if err := runtimeInstallTable(l.ctx, l.pool, table); err != nil {
			return nil, fmt.Errorf("pgxevents: install trigger for %q: %w", table, err)
		}
	} else {
		if err := validateTableTrigger(l.ctx, l.pool, table); err != nil {
			return nil, err
		}
	}

	sub := &subscription{
		table:    table,
		ch:       make(chan Event, l.cfg.subscriberBuf),
		listener: l,
	}
	l.subs[table] = append(l.subs[table], sub)
	return sub, nil
}

// Health returns a channel that emits errors on listener failure and nil
// on recovery. Buffer size 1; if the consumer is not reading, additional
// emissions are dropped silently.
func (l *listener) Health() <-chan error { return l.healthCh }

// Close stops the listener, closes all subscriptions, drains in-flight
// goroutines, and releases resources. Idempotent.
func (l *listener) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()

	l.cancel()
	l.wg.Wait()

	l.mu.Lock()
	for _, subs := range l.subs {
		for _, sub := range subs {
			sub.closeInternal()
		}
	}
	l.subs = nil
	close(l.healthCh)
	l.mu.Unlock()

	return nil
}

// emitHealth pushes a non-blocking signal to the health channel. Drops
// the signal if no consumer is reading.
func (l *listener) emitHealth(err error) {
	select {
	case l.healthCh <- err:
	default:
	}
}

// removeSubscription detaches sub from the table's subscriber list.
// Safe to call from any goroutine; no-op if already removed or if the
// listener is closed.
func (l *listener) removeSubscription(sub *subscription) {
	l.mu.Lock()
	defer l.mu.Unlock()
	list := l.subs[sub.table]
	for i, s := range list {
		if s == sub {
			l.subs[sub.table] = append(list[:i], list[i+1:]...)
			return
		}
	}
}
