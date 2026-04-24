package pgxevents

import (
	"math"
	"testing"
	"time"
)

func TestNextBackoff(t *testing.T) {
	max := 30 * time.Second
	cases := []struct {
		curr time.Duration
		want time.Duration
	}{
		{500 * time.Millisecond, time.Second},
		{time.Second, 2 * time.Second},
		{8 * time.Second, 16 * time.Second},
		{16 * time.Second, max}, // doubled exceeds max
		{max, max},              // already at cap
		{2 * max, max},
		{time.Duration(math.MaxInt64), max}, // overflow on multiply
	}
	for _, c := range cases {
		got := nextBackoff(c.curr, max)
		if got != c.want {
			t.Errorf("nextBackoff(%v, %v) = %v, want %v", c.curr, max, got, c.want)
		}
	}
}

func TestJitterRange(t *testing.T) {
	base := time.Second
	frac := 0.2
	min := time.Duration(float64(base) * (1 - frac))
	max := time.Duration(float64(base) * (1 + frac))
	for i := 0; i < 200; i++ {
		got := jitter(base, frac)
		if got < min || got > max {
			t.Errorf("jitter(%v, %v) = %v out of [%v, %v]", base, frac, got, min, max)
		}
	}
}

func TestJitterFractionZeroReturnsBase(t *testing.T) {
	base := 750 * time.Millisecond
	if got := jitter(base, 0); got != base {
		t.Errorf("jitter(%v, 0) = %v, want %v", base, got, base)
	}
	if got := jitter(base, -0.5); got != base {
		t.Errorf("jitter(%v, -0.5) = %v, want %v (negative fraction treated as zero)", base, got, base)
	}
}

func TestJitterFractionClampedToOne(t *testing.T) {
	base := time.Second
	for i := 0; i < 50; i++ {
		got := jitter(base, 5.0) // clamped to 1.0
		// At fraction=1, range is [0, 2*base]. Never negative.
		if got < 0 {
			t.Errorf("jitter went negative: %v", got)
		}
		if got > 2*base {
			t.Errorf("jitter exceeded 2*base: %v", got)
		}
	}
}
