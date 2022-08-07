package pgxevents

import "time"

type Option func(l *pgxListener)

func WithMaxRetries(n int) Option {
	return func(l *pgxListener) {
		l.maxRetries = n
	}
}

func WithDelay(t time.Duration) Option {
	return func(l *pgxListener) {
		l.delay = t
	}
}
