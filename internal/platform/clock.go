package platform

import (
	"context"
	"sync"
	"time"
)

const (
	defaultCommitWindow = 5 * time.Second
	minimumCommitWindow = 250 * time.Millisecond
)

type Clock interface {
	Now() time.Time
}

type SystemClock struct{}

func (SystemClock) Now() time.Time {
	return time.Now().UTC()
}

type ManualClock struct {
	mu  sync.RWMutex
	now time.Time
}

func NewManualClock(value time.Time) *ManualClock {
	return &ManualClock{now: value.UTC()}
}

func (c *ManualClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *ManualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

type commitContext struct {
	context.Context
	deadline time.Time
	done     chan struct{}
	timer    *time.Timer
	once     sync.Once
	mu       sync.RWMutex
	err      error
}

func NewCommitContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	window := commitWindow(parent)
	ctx := &commitContext{
		Context:  parent,
		deadline: time.Now().Add(window),
		done:     make(chan struct{}),
	}
	ctx.timer = time.AfterFunc(window, func() {
		ctx.finish(context.DeadlineExceeded)
	})
	cancel := func() {
		if ctx.timer != nil {
			ctx.timer.Stop()
		}
		ctx.finish(context.Canceled)
	}
	return ctx, cancel
}

func commitWindow(parent context.Context) time.Duration {
	window := defaultCommitWindow
	if deadline, ok := parent.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < window {
			window = remaining
		}
	}
	if window < minimumCommitWindow {
		return minimumCommitWindow
	}
	return window
}

func (c *commitContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func (c *commitContext) Done() <-chan struct{} {
	return c.done
}

func (c *commitContext) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

func (c *commitContext) finish(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		close(c.done)
	})
}
