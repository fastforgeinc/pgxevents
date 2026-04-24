package pgxevents

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"
)

// cleanupAdvisoryLockKey is the Postgres advisory-lock key used to elect
// a single listener pod for cleanup work per tick. Computed from a fixed
// string so all pgxevents instances across the fleet share the same lock.
var cleanupAdvisoryLockKey = func() int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("pgxevents:cleanup"))
	// Cast to int64; advisory lock accepts any 64-bit value.
	return int64(h.Sum64())
}()

// cleanupLoop runs as a background goroutine, periodically deleting
// expired outbox rows. Uses pg_try_advisory_lock so only one listener
// pod performs cleanup per tick across the fleet.
func (l *listener) cleanupLoop() {
	if l.cfg.cleanupInterval <= 0 {
		return
	}
	t := time.NewTicker(l.cfg.cleanupInterval)
	defer t.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-t.C:
			if err := l.cleanupOnce(l.ctx); err != nil {
				l.cfg.logger.Warn("pgxevents: cleanup error", "error", err)
			}
		}
	}
}

// cleanupOnce attempts to acquire the advisory lock and delete expired
// outbox rows. Returns nil without doing work if another instance holds
// the lock.
func (l *listener) cleanupOnce(ctx context.Context) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx,
		"SELECT pg_try_advisory_lock($1)", cleanupAdvisoryLockKey,
	).Scan(&locked); err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}
	if !locked {
		return nil
	}
	defer func() {
		// Best-effort release; lock is also released if the connection
		// returns to the pool and gets reset.
		_, _ = conn.Exec(ctx,
			"SELECT pg_advisory_unlock($1)", cleanupAdvisoryLockKey,
		)
	}()

	cutoff := time.Now().Add(-l.cfg.cleanupTTL)
	tag, err := conn.Exec(ctx, `
		DELETE FROM pgxevents_outbox
		WHERE created_at < $1
	`, cutoff)
	if err != nil {
		return fmt.Errorf("delete expired rows: %w", err)
	}
	rows := int(tag.RowsAffected())
	if rows > 0 {
		l.cfg.logger.Debug("pgxevents: cleanup deleted rows", "rows", rows)
	}
	l.cfg.metrics.OutboxCleanupRows(rows)
	return nil
}
