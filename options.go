package pgxevents

import "time"

// Option configures a Listener at construction. Options are applied in
// order; later options for the same setting overwrite earlier ones.
type Option func(*config)

// config is the internal settings struct populated by Options.
type config struct {
	runtimeInstall  bool
	loggedOutbox    bool
	backoff         BackoffConfig
	subscriberBuf   int
	overflowPolicy  OverflowPolicy
	cleanupTTL      time.Duration
	cleanupInterval time.Duration
	logger          Logger
	metrics         Metrics
}

// defaults returns the baseline config applied before user options.
func defaults() config {
	return config{
		runtimeInstall: false,
		loggedOutbox:   false,
		backoff: BackoffConfig{
			InitialDelay: 500 * time.Millisecond,
			MaxDelay:     30 * time.Second,
			Jitter:       0.2,
		},
		subscriberBuf:   64,
		overflowPolicy:  OverflowDropOldest,
		cleanupTTL:      time.Hour,
		cleanupInterval: 5 * time.Minute,
		logger:          NoopLogger{},
		metrics:         NoopMetrics{},
	}
}

// BackoffConfig configures exponential-backoff-with-jitter used when the
// listener reconnects after a LISTEN connection drops.
type BackoffConfig struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	// Jitter is the fractional randomisation applied to each delay, in
	// [0, 1]. The final delay falls in [base*(1-Jitter), base*(1+Jitter)].
	Jitter float64
}

// OverflowPolicy controls how per-subscriber channel overflow is handled
// when a subscriber is slower than event arrival.
type OverflowPolicy int

const (
	// OverflowDropOldest drops the oldest buffered event to make room for
	// the new one. The subscriber stays connected but loses events.
	OverflowDropOldest OverflowPolicy = iota

	// OverflowDisconnect closes the subscription. The consumer's Events
	// channel is closed; they must resubscribe.
	OverflowDisconnect
)

// WithRuntimeInstall controls whether NewListener installs the pgxevents
// DDL (function, outbox table, index) at startup. Default: false —
// migrations are required.
//
// When true, NewListener runs InstallSQL on the pool and each Listen
// call additionally runs the per-table trigger SQL. Requires the database
// user to hold DDL privileges.
func WithRuntimeInstall(install bool) Option {
	return func(c *config) { c.runtimeInstall = install }
}

// WithLoggedOutbox switches the outbox table from UNLOGGED (default) to
// WAL-logged storage. Typically unnecessary — outbox durability across
// crashes is not required, since a crashed PostgreSQL kills LISTEN
// sessions and recovery happens via subscribers' own reconnection paths.
// Enable only if you need outbox rows replicated to standbys or preserved
// across crashes for auditing.
//
// Applies only at DDL install time. Has no effect when the outbox table
// already exists.
func WithLoggedOutbox() Option {
	return func(c *config) { c.loggedOutbox = true }
}

// WithBackoff sets the reconnect backoff policy.
func WithBackoff(cfg BackoffConfig) Option {
	return func(c *config) { c.backoff = cfg }
}

// WithSubscriberBuffer sets the per-subscriber channel buffer size.
// Default: 64. Larger buffers tolerate bursty consumers at the cost of
// memory; smaller buffers surface slow consumers sooner.
func WithSubscriberBuffer(n int) Option {
	return func(c *config) { c.subscriberBuf = n }
}

// WithOverflowPolicy sets the per-subscriber overflow behaviour.
func WithOverflowPolicy(p OverflowPolicy) Option {
	return func(c *config) { c.overflowPolicy = p }
}

// WithCleanupTTL sets the age at which outbox rows become eligible for
// deletion by the background cleanup loop. Default: 1h. Must exceed the
// worst-case NOTIFY-to-deliver latency; typical delivery is sub-second.
func WithCleanupTTL(ttl time.Duration) Option {
	return func(c *config) { c.cleanupTTL = ttl }
}

// WithCleanupInterval sets how often the cleanup loop attempts to trim
// expired outbox rows. Default: 5m. Cleanup uses a PostgreSQL advisory
// lock so only one listener pod performs cleanup per tick across the
// fleet.
func WithCleanupInterval(d time.Duration) Option {
	return func(c *config) { c.cleanupInterval = d }
}

// WithLogger attaches a logger for internal events (reconnects, skipped
// malformed events, cleanup results). Default: NoopLogger.
func WithLogger(l Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithMetrics attaches a metrics sink for counters and gauges.
// Default: NoopMetrics.
func WithMetrics(m Metrics) Option {
	return func(c *config) { c.metrics = m }
}
