package policy

import "time"

// Clock keeps the policy independent of wall time and goroutine scheduling.
type Clock interface{ Now() time.Time }
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

type ManualClock struct{ Time time.Time }

func (c *ManualClock) Now() time.Time  { return c.Time }
func (c *ManualClock) Set(t time.Time) { c.Time = t }
