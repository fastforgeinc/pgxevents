package pgxevents

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
)

// listenLoop runs as a background goroutine, maintaining the LISTEN
// connection and dispatching notifications until the listener is closed.
func (l *listener) listenLoop() {
	delay := l.cfg.backoff.InitialDelay

	for {
		if l.ctx.Err() != nil {
			return
		}

		err := l.listenOnce()
		if errors.Is(err, context.Canceled) || l.ctx.Err() != nil {
			return
		}
		if err != nil {
			l.cfg.logger.Warn("pgxevents: listener error", "error", err)
			l.cfg.metrics.Reconnect(err)
			l.cfg.metrics.ListenerDown(err)
			l.emitHealth(err)
		}

		// Backoff before reconnect.
		sleep := jitter(delay, l.cfg.backoff.Jitter)
		select {
		case <-l.ctx.Done():
			return
		case <-time.After(sleep):
		}
		delay = nextBackoff(delay, l.cfg.backoff.MaxDelay)
	}
}

// listenOnce acquires a connection, runs LISTEN, and reads notifications
// until an error occurs or the context is cancelled. Returns the error
// that ended the connection.
func (l *listener) listenOnce() error {
	conn, err := l.pool.Acquire(l.ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(l.ctx, "LISTEN "+NotifyChannel); err != nil {
		return fmt.Errorf("LISTEN: %w", err)
	}

	l.cfg.metrics.ListenerUp()
	l.emitHealth(nil)

	for {
		notif, err := conn.Conn().WaitForNotification(l.ctx)
		if err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		if notif.Channel != NotifyChannel {
			continue
		}
		l.handleNotification(notif.Payload)
	}
}

// handleNotification fetches the outbox snapshot referenced by outboxID
// and fans it out to subscribers of the snapshot's table.
func (l *listener) handleNotification(outboxID string) {
	var (
		id        string
		tableName string
		action    string
		data      []byte
		createdAt time.Time
	)
	err := l.pool.QueryRow(l.ctx, `
		SELECT id::text, table_name, action, data, created_at
		FROM pgxevents_outbox
		WHERE id = $1
	`, outboxID).Scan(&id, &tableName, &action, &data, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		l.cfg.logger.Warn("pgxevents: outbox snapshot missing", "outbox_id", outboxID)
		l.cfg.metrics.EventDropped("", "snapshot_missing")
		return
	}
	if err != nil {
		l.cfg.logger.Warn("pgxevents: fetch outbox snapshot", "outbox_id", outboxID, "error", err)
		l.cfg.metrics.EventDropped("", "fetch_error")
		return
	}

	ev := Event{
		Table:  tableName,
		Action: Action(action),
		Data:   data,
		Meta:   Meta{OutboxID: id, CreatedAt: createdAt},
	}
	l.cfg.metrics.EventReceived(tableName)
	l.fanout(ev)
}

// fanout delivers ev to all current subscribers of ev.Table. Subscribers
// that signal not-alive (e.g. OverflowDisconnect) are removed and closed.
func (l *listener) fanout(ev Event) {
	l.mu.Lock()
	subs := append([]*subscription(nil), l.subs[ev.Table]...)
	l.mu.Unlock()

	var dead []*subscription
	for _, sub := range subs {
		if !sub.trySend(ev, l.cfg.overflowPolicy, l.cfg.metrics, l.cfg.logger) {
			dead = append(dead, sub)
		}
	}
	for _, sub := range dead {
		l.removeSubscription(sub)
		sub.closeInternal()
	}
}

// nextBackoff returns the next delay in an exponential backoff capped at
// max. Negative or overflowing results are clamped to max.
func nextBackoff(curr, max time.Duration) time.Duration {
	next := curr * 2
	if next <= 0 || next > max {
		return max
	}
	return next
}

// jitter applies fractional symmetric randomisation in [-fraction, +fraction]
// to d. fraction is clamped to [0, 1].
func jitter(d time.Duration, fraction float64) time.Duration {
	if fraction <= 0 {
		return d
	}
	if fraction > 1 {
		fraction = 1
	}
	delta := float64(d) * fraction
	offset := (rand.Float64()*2 - 1) * delta
	out := d + time.Duration(offset)
	if out < 0 {
		return 0
	}
	return out
}
