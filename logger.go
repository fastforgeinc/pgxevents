package pgxevents

// Logger is the minimal logging surface used by the library. Adapters for
// common loggers (zap, slog) are consumer-provided. Default: NoopLogger.
type Logger interface {
	Debug(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// NoopLogger discards all messages. Use when logging is handled elsewhere
// or in tests.
type NoopLogger struct{}

func (NoopLogger) Debug(string, ...any) {}
func (NoopLogger) Info(string, ...any)  {}
func (NoopLogger) Warn(string, ...any)  {}
func (NoopLogger) Error(string, ...any) {}
