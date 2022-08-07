package pgxevents

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync"
	"time"
)

type pgxListener struct {
	ctx  context.Context
	pool *pgxpool.Pool
	subs map[string][]chan *Event
	mux  sync.RWMutex

	delay      time.Duration
	attempt    int
	maxRetries int
}

const (
	defaultDelay      = 5 * time.Second
	defaultMaxRetries = 0
)

func NewListener(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*pgxListener, error) {
	l := pgxListener{
		ctx:        ctx,
		pool:       pool,
		subs:       map[string][]chan *Event{},
		delay:      defaultDelay,
		maxRetries: defaultMaxRetries,
	}
	for _, opt := range opts {
		opt(&l)
	}

	if _, err := pool.Exec(ctx, procedure()); err != nil {
		return nil, err
	}

	go l.run(ctx)

	return &l, nil
}

func (l *pgxListener) Listen(table string) (chan *Event, error) {
	sub := make(chan *Event)
	l.mux.Lock()
	defer l.mux.Unlock()
	if _, ok := l.subs[table]; !ok {
		if _, err := l.pool.Exec(l.ctx, trigger(table)); err != nil {
			return nil, err
		}
		l.subs[table] = make([]chan *Event, 0)
	}
	l.subs[table] = append(l.subs[table], sub)
	return sub, nil
}

func (l *pgxListener) run(ctx context.Context) {
	for {
		conn, err := l.acquire(ctx)
		if err != nil {
			break
		}
		if _, err := conn.Exec(ctx, listen()); err != nil {
			conn.Release()
			break
		}
		l.listen(ctx, conn)
		conn.Release()
	}
	l.close()
}

func (l *pgxListener) listen(ctx context.Context, conn *pgxpool.Conn) {
	for {
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			break
		}
		if notification.Channel != channel {
			continue
		}
		event := new(Event)
		if err := json.Unmarshal([]byte(notification.Payload), event); err == nil {
			l.publish(event)
		}
	}
}

func (l *pgxListener) close() {
	l.mux.Lock()
	defer l.mux.Unlock()
	for table := range l.subs {
		for _, ch := range l.subs[table] {
			close(ch)
		}
		delete(l.subs, table)
	}
}

func (l *pgxListener) acquire(ctx context.Context) (*pgxpool.Conn, error) {
	for l.maxRetries == 0 || l.attempt < l.maxRetries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		conn, err := l.pool.Acquire(ctx)
		if err == nil {
			if err = conn.Ping(ctx); err == nil {
				l.attempt = 0
				return conn, nil
			}
		}
		l.attempt++
		time.Sleep(l.delay)
	}
	return nil, MaxRetriesError{l.maxRetries}
}

func (l *pgxListener) publish(event *Event) {
	l.mux.RLock()
	defer l.mux.RUnlock()
	if chs, ok := l.subs[event.Table]; ok {
		for _, ch := range chs {
			ch <- event
		}
	}
}
