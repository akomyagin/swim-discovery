// fakeClock is the test-side Clock: virtual time that moves only via Advance,
// which synchronously delivers every firing whose deadline has been reached —
// so a test can assert node state immediately after Advance, with no sleeping
// and no polling. It is the key instrument of the Этап 4 suspicion tests.
package swim

import (
	"sort"
	"sync"
	"testing"
	"time"
)

var _ Clock = (*fakeClock)(nil) // same compile-time contract as systemClock

// fakeWaiter is one pending After channel or AfterFunc callback.
type fakeWaiter struct {
	deadline time.Time
	seq      int // registration order; tie-break for deterministic firing
	ch       chan time.Time
	fn       func()
	stopped  bool // fired or cancelled — either way, Stop must report false
}

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	seq     int
	waiters []*fakeWaiter
}

func newFakeClock() *fakeClock {
	// Arbitrary fixed epoch: tests reason in durations, never wall time.
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	c.waiters = append(c.waiters, &fakeWaiter{deadline: c.now.Add(d), seq: c.seq, ch: ch})
	return ch
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	w := &fakeWaiter{deadline: c.now.Add(d), seq: c.seq, fn: f}
	c.waiters = append(c.waiters, w)
	return &fakeTimer{c: c, w: w}
}

// Advance moves virtual time by d and synchronously fires every waiter whose
// deadline has been reached (deadline <= new now), in deadline order with
// registration order as the tie-break. Callbacks run on the caller's
// goroutine with NO clock lock held — they may take Node locks and even
// register new timers (which then need a further Advance to fire).
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due, rest []*fakeWaiter
	for _, w := range c.waiters {
		if !w.deadline.After(now) {
			w.stopped = true // consumed: a late Stop must report false
			due = append(due, w)
		} else {
			rest = append(rest, w)
		}
	}
	c.waiters = rest
	c.mu.Unlock()

	sort.Slice(due, func(i, j int) bool {
		if !due[i].deadline.Equal(due[j].deadline) {
			return due[i].deadline.Before(due[j].deadline)
		}
		return due[i].seq < due[j].seq
	})
	for _, w := range due {
		if w.ch != nil {
			w.ch <- now // buffered(1), single send per waiter: never blocks
		}
		if w.fn != nil {
			w.fn()
		}
	}
}

type fakeTimer struct {
	c *fakeClock
	w *fakeWaiter
}

// Stop cancels the pending firing; false means it already fired or was
// already stopped — the time.Timer.Stop contract. Like time.AfterFunc, a
// callback that Advance has already popped (but not yet run) can still
// execute after a Stop that returned false; Node-side incarnation checks
// plus Merge precedence make that race harmless.
func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	if t.w.stopped {
		return false
	}
	t.w.stopped = true
	for i, w := range t.c.waiters {
		if w == t.w {
			t.c.waiters = append(t.c.waiters[:i], t.c.waiters[i+1:]...)
			break
		}
	}
	return true
}

func TestFakeClock_AdvanceFiresOnlyDue(t *testing.T) {
	c := newFakeClock()
	early := c.After(100 * time.Millisecond)
	late := c.After(200 * time.Millisecond)

	c.Advance(50 * time.Millisecond)
	select {
	case <-early:
		t.Fatal("early fired 50ms before its deadline")
	case <-late:
		t.Fatal("late fired 150ms before its deadline")
	default:
	}

	// Cumulative: 50+50 = exactly the early deadline; late still pending.
	c.Advance(50 * time.Millisecond)
	select {
	case <-early:
	default:
		t.Fatal("early did not fire at its exact deadline (Advance must accumulate)")
	}
	select {
	case <-late:
		t.Fatal("late fired 100ms before its deadline")
	default:
	}

	c.Advance(100 * time.Millisecond)
	select {
	case <-late:
	default:
		t.Fatal("late did not fire at its deadline")
	}
}

func TestFakeClock_AfterFuncFiresSynchronouslyAndStops(t *testing.T) {
	c := newFakeClock()
	fired := 0
	live := c.AfterFunc(time.Second, func() { fired++ })
	cancelled := c.AfterFunc(2*time.Second, func() { t.Error("stopped timer fired") })

	if !cancelled.Stop() {
		t.Error("Stop on a pending timer must report true")
	}
	c.Advance(time.Second)
	if fired != 1 {
		t.Fatalf("fired = %d right after Advance, want 1 (delivery must be synchronous)", fired)
	}
	if live.Stop() {
		t.Error("Stop after firing must report false")
	}
	c.Advance(5 * time.Second)
	if fired != 1 {
		t.Errorf("fired = %d after extra Advance, want still 1 (a timer fires once)", fired)
	}
}

// TestFakeClock_ConcurrentUse hammers registration, Stop and Advance from
// concurrent goroutines: suspicion timers get armed from probe goroutines and
// the receive goroutine while tests advance time, so the clock itself must be
// race-free under -race.
func TestFakeClock_ConcurrentUse(t *testing.T) {
	c := newFakeClock()
	var fired sync.WaitGroup
	var users sync.WaitGroup
	for i := 0; i < 8; i++ {
		users.Add(1)
		go func() {
			defer users.Done()
			for j := 0; j < 50; j++ {
				d := time.Duration(j%5+1) * time.Millisecond
				switch j % 3 {
				case 0:
					fired.Add(1)
					done := false
					var mu sync.Mutex
					tm := c.AfterFunc(d, func() {
						mu.Lock()
						defer mu.Unlock()
						if !done {
							done = true
							fired.Done()
						}
					})
					if j%6 == 0 && tm.Stop() {
						mu.Lock()
						if !done {
							done = true
							fired.Done()
						}
						mu.Unlock()
					}
				default:
					c.After(d)
				}
			}
		}()
	}
	users.Add(1)
	go func() {
		defer users.Done()
		for k := 0; k < 20; k++ {
			c.Advance(time.Millisecond)
		}
	}()
	users.Wait()
	c.Advance(time.Second) // flush everything still pending
	fired.Wait()           // every un-stopped callback must have run exactly once
}
