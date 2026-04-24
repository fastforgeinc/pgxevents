package pgxevents

import (
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	c := defaults()

	if c.runtimeInstall {
		t.Error("runtimeInstall should default to false")
	}
	if c.loggedOutbox {
		t.Error("loggedOutbox should default to false")
	}
	if c.backoff.InitialDelay != 500*time.Millisecond {
		t.Errorf("backoff.InitialDelay = %v, want 500ms", c.backoff.InitialDelay)
	}
	if c.backoff.MaxDelay != 30*time.Second {
		t.Errorf("backoff.MaxDelay = %v, want 30s", c.backoff.MaxDelay)
	}
	if c.backoff.Jitter != 0.2 {
		t.Errorf("backoff.Jitter = %v, want 0.2", c.backoff.Jitter)
	}
	if c.subscriberBuf != 64 {
		t.Errorf("subscriberBuf = %d, want 64", c.subscriberBuf)
	}
	if c.overflowPolicy != OverflowDropOldest {
		t.Errorf("overflowPolicy = %v, want OverflowDropOldest", c.overflowPolicy)
	}
	if c.cleanupTTL != time.Hour {
		t.Errorf("cleanupTTL = %v, want 1h", c.cleanupTTL)
	}
	if c.cleanupInterval != 5*time.Minute {
		t.Errorf("cleanupInterval = %v, want 5m", c.cleanupInterval)
	}
	if _, ok := c.logger.(NoopLogger); !ok {
		t.Errorf("default logger type = %T, want NoopLogger", c.logger)
	}
	if _, ok := c.metrics.(NoopMetrics); !ok {
		t.Errorf("default metrics type = %T, want NoopMetrics", c.metrics)
	}
}

func TestOptionsApply(t *testing.T) {
	type recordingLogger struct{ NoopLogger }
	type recordingMetrics struct{ NoopMetrics }

	c := defaults()
	WithRuntimeInstall(true)(&c)
	WithLoggedOutbox()(&c)
	WithBackoff(BackoffConfig{InitialDelay: time.Second, MaxDelay: time.Minute, Jitter: 0.5})(&c)
	WithSubscriberBuffer(128)(&c)
	WithOverflowPolicy(OverflowDisconnect)(&c)
	WithCleanupTTL(2 * time.Hour)(&c)
	WithCleanupInterval(10 * time.Minute)(&c)
	WithLogger(recordingLogger{})(&c)
	WithMetrics(recordingMetrics{})(&c)

	if !c.runtimeInstall {
		t.Error("WithRuntimeInstall(true) did not set runtimeInstall")
	}
	if !c.loggedOutbox {
		t.Error("WithLoggedOutbox did not set loggedOutbox")
	}
	if c.backoff.InitialDelay != time.Second || c.backoff.MaxDelay != time.Minute || c.backoff.Jitter != 0.5 {
		t.Errorf("WithBackoff did not apply: %+v", c.backoff)
	}
	if c.subscriberBuf != 128 {
		t.Errorf("WithSubscriberBuffer = %d, want 128", c.subscriberBuf)
	}
	if c.overflowPolicy != OverflowDisconnect {
		t.Errorf("WithOverflowPolicy = %v, want OverflowDisconnect", c.overflowPolicy)
	}
	if c.cleanupTTL != 2*time.Hour {
		t.Errorf("WithCleanupTTL = %v, want 2h", c.cleanupTTL)
	}
	if c.cleanupInterval != 10*time.Minute {
		t.Errorf("WithCleanupInterval = %v, want 10m", c.cleanupInterval)
	}
	if _, ok := c.logger.(recordingLogger); !ok {
		t.Errorf("WithLogger type = %T, want recordingLogger", c.logger)
	}
	if _, ok := c.metrics.(recordingMetrics); !ok {
		t.Errorf("WithMetrics type = %T, want recordingMetrics", c.metrics)
	}
}

func TestOptionsLastWriteWins(t *testing.T) {
	c := defaults()
	WithSubscriberBuffer(10)(&c)
	WithSubscriberBuffer(20)(&c)
	if c.subscriberBuf != 20 {
		t.Errorf("subscriberBuf = %d, want 20 (last write wins)", c.subscriberBuf)
	}
}
