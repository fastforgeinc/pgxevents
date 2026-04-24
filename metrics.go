package pgxevents

// Metrics is the sink for library-observed counters and gauges. Consumers
// implement this to bridge pgxevents into their own metrics registry
// (Prometheus, OpenTelemetry, etc.). Default: NoopMetrics.
type Metrics interface {
	// EventReceived increments when a notification-fetched snapshot is
	// successfully parsed and ready to fan out.
	EventReceived(table string)

	// EventDropped increments when an event is not delivered to a
	// subscriber. Reason describes the cause: "overflow", "decode_error",
	// "snapshot_missing", etc.
	EventDropped(table, reason string)

	// Reconnect is called each time the LISTEN connection must be
	// re-established. Err is the reason for the previous connection's
	// failure.
	Reconnect(err error)

	// ListenerUp is called when the LISTEN connection is established
	// successfully (including initial connect and each recovery).
	ListenerUp()

	// ListenerDown is called when the LISTEN connection drops.
	ListenerDown(err error)

	// OutboxCleanupRows reports the number of rows deleted by a cleanup
	// pass.
	OutboxCleanupRows(n int)
}

// NoopMetrics discards all metric emissions. Use when metrics are
// handled elsewhere or in tests.
type NoopMetrics struct{}

func (NoopMetrics) EventReceived(string)        {}
func (NoopMetrics) EventDropped(string, string) {}
func (NoopMetrics) Reconnect(error)             {}
func (NoopMetrics) ListenerUp()                 {}
func (NoopMetrics) ListenerDown(error)          {}
func (NoopMetrics) OutboxCleanupRows(int)       {}
