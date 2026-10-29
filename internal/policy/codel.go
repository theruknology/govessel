package policy

import (
	"math"
	"time"
)

// codel applies the CoDel persistence test and inverse-square-root drop schedule.
type codel struct {
	firstAbove, next time.Time
	dropping         bool
	count            int
}

func (c *codel) drop(now time.Time, sojourn, target, interval time.Duration, backlog bool) bool {
	if sojourn < target || !backlog {
		c.firstAbove = time.Time{}
		c.dropping = false
		return false
	}
	if c.firstAbove.IsZero() {
		c.firstAbove = now.Add(interval)
		return false
	}
	if now.Before(c.firstAbove) {
		return false
	}
	if !c.dropping {
		c.dropping = true
		c.count = 1
		c.next = now.Add(interval)
		return true
	}
	if now.Before(c.next) {
		return false
	}
	c.count++
	c.next = c.next.Add(time.Duration(float64(interval) / math.Sqrt(float64(c.count))))
	return true
}
