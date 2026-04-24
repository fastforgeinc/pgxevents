package pgxevents

import (
	"encoding/json"
	"fmt"
	"sync"
)

// ListenTyped subscribes to the given table and returns a TypedSubscription
// whose events carry a *T populated from the row snapshot.
//
// T should be a struct whose field names or json tags match the table's
// columns. Unmarshalling uses encoding/json; rows that fail to decode are
// logged via the configured Logger, counted via the configured Metrics
// (with reason "decode_error"), and skipped — the subscription stays
// alive.
//
// ListenTyped is a top-level function rather than a method on Listener
// because Go does not allow type parameters on interface methods. It only
// supports listeners returned by NewListener.
func ListenTyped[T any](l Listener, table string) (TypedSubscription[T], error) {
	cl, ok := l.(*listener)
	if !ok {
		return nil, fmt.Errorf("pgxevents: ListenTyped supports only listeners returned by NewListener")
	}
	raw, err := cl.Listen(table)
	if err != nil {
		return nil, err
	}
	ts := &typedSubscription[T]{
		raw:     raw,
		ch:      make(chan TypedEvent[T], cl.cfg.subscriberBuf),
		logger:  cl.cfg.logger,
		metrics: cl.cfg.metrics,
		table:   table,
	}
	go ts.run()
	return ts, nil
}

// typedSubscription wraps a raw Subscription with a JSON decoder loop.
type typedSubscription[T any] struct {
	raw     Subscription
	ch      chan TypedEvent[T]
	logger  Logger
	metrics Metrics
	table   string

	closeOnce sync.Once
}

func (t *typedSubscription[T]) Events() <-chan TypedEvent[T] { return t.ch }

func (t *typedSubscription[T]) Close() error {
	t.closeOnce.Do(func() {
		_ = t.raw.Close()
	})
	return nil
}

// run forwards decoded events to t.ch. Exits and closes t.ch when the
// raw subscription's channel is closed.
func (t *typedSubscription[T]) run() {
	defer close(t.ch)
	for ev := range t.raw.Events() {
		var row T
		if err := json.Unmarshal(ev.Data, &row); err != nil {
			t.logger.Warn("pgxevents: decode error",
				"table", t.table,
				"outbox_id", ev.Meta.OutboxID,
				"error", err,
			)
			t.metrics.EventDropped(t.table, "decode_error")
			continue
		}
		t.ch <- TypedEvent[T]{
			Table:  ev.Table,
			Action: ev.Action,
			Row:    &row,
			Meta:   ev.Meta,
		}
	}
}
