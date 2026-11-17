package session

import (
	"sync"
	"time"
)

// ReannounceRequestResult reports whether a manual request was queued.
type ReannounceRequestResult struct {
	Accepted   bool
	RetryAfter time.Duration
}

// ReannounceController serializes manual tracker announces and applies a
// cooldown shared with startup and periodic attempts.
type ReannounceController struct {
	mu          sync.Mutex
	cooldown    time.Duration
	lastAttempt time.Time
	inFlight    bool
	requests    chan struct{}
	now         func() time.Time
}

func NewReannounceController(cooldown time.Duration) *ReannounceController {
	return &ReannounceController{
		cooldown: cooldown,
		requests: make(chan struct{}, 1),
		now:      time.Now,
	}
}

// MarkAttempt records a startup or scheduled announce attempt.
func (c *ReannounceController) MarkAttempt() {
	c.mu.Lock()
	c.lastAttempt = c.now()
	c.inFlight = true
	c.mu.Unlock()
}

// FinishAttempt allows another announce after the cooldown expires.
func (c *ReannounceController) FinishAttempt() {
	c.mu.Lock()
	c.inFlight = false
	c.mu.Unlock()
}

// Request queues a manual reannounce without blocking the caller.
func (c *ReannounceController) Request() ReannounceRequestResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inFlight {
		return ReannounceRequestResult{RetryAfter: c.remainingLocked()}
	}
	if remaining := c.remainingLocked(); remaining > 0 {
		return ReannounceRequestResult{RetryAfter: remaining}
	}
	c.lastAttempt = c.now()
	c.inFlight = true
	select {
	case c.requests <- struct{}{}:
		return ReannounceRequestResult{Accepted: true}
	default:
		c.inFlight = false
		return ReannounceRequestResult{}
	}
}

func (c *ReannounceController) Requests() <-chan struct{} { return c.requests }

func (c *ReannounceController) remainingLocked() time.Duration {
	if c.lastAttempt.IsZero() || c.cooldown <= 0 {
		return 0
	}
	remaining := c.cooldown - c.now().Sub(c.lastAttempt)
	if remaining < 0 {
		return 0
	}
	return remaining
}
