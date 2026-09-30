package clock

import "time"

// Clock abstracts time so suppression windows, maintenance windows and
// step timeouts are deterministic in tests.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

// RealClock uses wall-clock time.
type RealClock struct{}

func NewRealClock() *RealClock { return &RealClock{} }

func (c *RealClock) Now() time.Time                         { return time.Now() }
func (c *RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (c *RealClock) Sleep(d time.Duration)                  { time.Sleep(d) }

// FakeClock is a manually controlled clock for tests.
type FakeClock struct {
	now time.Time
}

func NewFakeClock(start time.Time) *FakeClock { return &FakeClock{now: start} }

func (c *FakeClock) Now() time.Time { return c.now }

// After on a fake clock fires only after Advance reaches the deadline.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	deadline := c.now.Add(d)
	go func() {
		for {
			time.Sleep(time.Millisecond)
			if !c.Now().Before(deadline) {
				ch <- deadline
				return
			}
		}
	}()
	return ch
}

func (c *FakeClock) Sleep(d time.Duration) { c.Advance(d) }

// Advance moves the clock forward.
func (c *FakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// Set moves the clock to an absolute time.
func (c *FakeClock) Set(t time.Time) { c.now = t }
