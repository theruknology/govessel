package policy

import (
	"math"
	"testing"
	"time"
)

func testEngine(t *testing.T, edit func(*Config)) (*Engine, *ManualClock) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	if edit != nil {
		edit(&cfg)
	}
	clock := &ManualClock{Time: time.Unix(1, 0)}
	e, err := New(cfg, clock)
	if err != nil {
		t.Fatal(err)
	}
	return e, clock
}
func enqueue(t *testing.T, e *Engine, tenant string, max int) *Job {
	t.Helper()
	j, err := e.Enqueue(Job{Tenant: tenant, MaxOutputTokens: max, Deadline: e.clock.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func TestPredictorAndAdmission(t *testing.T) {
	e, clock := testEngine(t, func(c *Config) { c.InitialOutput = 100; c.Alpha = 0.5 })
	first := enqueue(t, e, "a", 1000)
	if first.Estimate != 100 {
		t.Fatalf("cold estimate %g", first.Estimate)
	}
	run, _ := e.Next()
	clock.Set(clock.Now().Add(400 * time.Millisecond))
	e.Complete(run.ID, 400, true)
	next := enqueue(t, e, "a", 1000)
	if next.Estimate != 250 {
		t.Fatalf("learned estimate %g", next.Estimate)
	}
	if other := enqueue(t, e, "b", 1000); other.Estimate != 100 {
		t.Fatal("predictor leaked across tenants")
	}
	_, err := e.Enqueue(Job{Tenant: "a", MaxOutputTokens: 1000, Deadline: clock.Now().Add(10 * time.Millisecond)})
	if ReasonOf(err) != Deadline {
		t.Fatalf("expected early deadline rejection, got %v", err)
	}
	capped := enqueue(t, e, "a", 10)
	if capped.Estimate != 10 {
		t.Fatal("output cap ignored")
	}
}
func TestFailedResponseDoesNotTrain(t *testing.T) {
	e, clock := testEngine(t, nil)
	enqueue(t, e, "a", 1024)
	j, _ := e.Next()
	clock.Set(clock.Now().Add(time.Second))
	e.Complete(j.ID, 1, false)
	j = enqueue(t, e, "a", 1024)
	if j.Estimate != 128 {
		t.Fatalf("failure trained predictor: %g", j.Estimate)
	}
	if e.Complete(999, 1, true) {
		t.Fatal("unknown completion accepted")
	}
}
func TestDRRWeightedTokenFairness(t *testing.T) {
	for _, weight := range []int{1, 2} {
		t.Run(string(rune('0'+weight)), func(t *testing.T) {
			e, clock := testEngine(t, func(c *Config) {
				c.MaxQueue = 12000
				c.MaxTenantQueue = 6000
				c.InitialOutput = 500
				c.Quantum = 100
				c.Weights = map[string]int{"large": weight}
				c.CoDelTarget = time.Hour
				c.CoDelInterval = 2 * time.Hour
			})
			for i := 0; i < 5000; i++ {
				enqueue(t, e, "small", 50)
				enqueue(t, e, "large", 500)
			}
			work := map[string]float64{}
			for i := 0; i < 2000; i++ {
				j, drops := e.Next()
				if j == nil || len(drops) > 0 {
					t.Fatal("unexpected empty dispatch or drops")
				}
				work[j.Tenant] += j.Estimate
				clock.Set(clock.Now().Add(time.Millisecond))
				e.Complete(j.ID, j.MaxOutputTokens, true)
			}
			ratio := work["large"] / work["small"]
			if math.Abs(ratio-float64(weight)) > 0.03 {
				t.Fatalf("weighted token ratio = %g; want %d", ratio, weight)
			}
		})
	}
}
func TestLimitsCancellationAndIdleEviction(t *testing.T) {
	e, _ := testEngine(t, func(c *Config) { c.MaxQueue = 2; c.MaxTenantQueue = 1; c.MaxTenants = 2; c.Baseline = true })
	a := enqueue(t, e, "a", 10)
	_, err := e.Enqueue(Job{Tenant: "a", MaxOutputTokens: 10, Deadline: e.clock.Now().Add(time.Hour)})
	if ReasonOf(err) != QueueFull {
		t.Fatal("per tenant limit not enforced")
	}
	b := enqueue(t, e, "b", 10)
	_, err = e.Enqueue(Job{Tenant: "c", MaxOutputTokens: 10, Deadline: e.clock.Now().Add(time.Hour)})
	if ReasonOf(err) != QueueFull {
		t.Fatal("global queue limit not enforced")
	}
	if e.Cancel(a.ID) == nil || e.Cancel(a.ID) != nil {
		t.Fatal("cancel not idempotent")
	}
	c := enqueue(t, e, "c", 10)
	if e.Snapshot().Tenants != 2 {
		t.Fatal("tenant state unbounded")
	}
	first, _ := e.Next()
	if first.ID != b.ID {
		t.Fatal("baseline is not FIFO after cancellation")
	}
	e.Complete(first.ID, 10, true)
	second, _ := e.Next()
	if second.ID != c.ID {
		t.Fatal("missing second job")
	}
	e.Complete(second.ID, 10, true)
	if s := e.Snapshot(); s.Queued != 0 || s.Running != 0 {
		t.Fatal(s)
	}
}
func TestTenantLimitWithAllTenantsBusy(t *testing.T) {
	e, _ := testEngine(t, func(c *Config) { c.MaxTenants = 1 })
	enqueue(t, e, "a", 10)
	_, err := e.Enqueue(Job{Tenant: "b", MaxOutputTokens: 10, Deadline: e.clock.Now().Add(time.Hour)})
	if ReasonOf(err) != TenantLimit {
		t.Fatalf("got %v", err)
	}
}
func TestCoDelPersistenceRecoveryAndControlLaw(t *testing.T) {
	var c codel
	target, interval := 50*time.Millisecond, 100*time.Millisecond
	now := time.Unix(1, 0)
	if c.drop(now, 60*time.Millisecond, target, interval, true) {
		t.Fatal("transient burst dropped")
	}
	if c.drop(now.Add(99*time.Millisecond), 200*time.Millisecond, target, interval, true) {
		t.Fatal("dropped before interval")
	}
	if !c.drop(now.Add(interval), 200*time.Millisecond, target, interval, true) {
		t.Fatal("persistent congestion not dropped")
	}
	if c.drop(now.Add(150*time.Millisecond), 250*time.Millisecond, target, interval, true) {
		t.Fatal("dropped before next control step")
	}
	if !c.drop(now.Add(200*time.Millisecond), 300*time.Millisecond, target, interval, true) {
		t.Fatal("missing scheduled drop")
	}
	if c.count != 2 || c.next.Sub(now.Add(200*time.Millisecond)) >= interval {
		t.Fatal("control law did not accelerate")
	}
	if c.drop(now.Add(time.Second), time.Millisecond, target, interval, true) || c.dropping {
		t.Fatal("did not recover")
	}
}
func TestCoDelDropsFromScheduler(t *testing.T) {
	e, clock := testEngine(t, func(c *Config) { c.Quantum = 10000 })
	for i := 0; i < 10; i++ {
		enqueue(t, e, "a", 10)
	}
	clock.Set(clock.Now().Add(60 * time.Millisecond))
	j, drops := e.Next()
	if len(drops) > 0 {
		t.Fatal("transient drop")
	}
	e.Complete(j.ID, 10, true)
	clock.Set(clock.Now().Add(110 * time.Millisecond))
	_, drops = e.Next()
	if len(drops) != 1 || drops[0].Reason != Shed {
		t.Fatalf("expected CoDel drop, got %v", drops)
	}
}
func TestExpiredQueueAndConcurrencyCap(t *testing.T) {
	e, clock := testEngine(t, nil)
	a := enqueue(t, e, "a", 10)
	b := enqueue(t, e, "b", 10)
	j, _ := e.Next()
	if j.ID != a.ID {
		t.Fatal("unexpected first job")
	}
	if j, _ := e.Next(); j != nil {
		t.Fatal("exceeded concurrency limit")
	}
	clock.Set(clock.Now().Add(2 * time.Hour))
	e.Complete(a.ID, 10, false)
	j, drops := e.Next()
	if j != nil || len(drops) != 1 || drops[0].Job.ID != b.ID || drops[0].Reason != Deadline {
		t.Fatal("expired request dispatched")
	}
}
func TestInvalidConfigAndInput(t *testing.T) {
	for _, edit := range []func(*Config){func(c *Config) { c.Quantum = 0 }, func(c *Config) { c.Alpha = math.NaN() }, func(c *Config) { c.SafetyFactor = math.Inf(1) }, func(c *Config) { c.Weights = map[string]int{"a": -1} }, func(c *Config) { c.CoDelInterval = c.CoDelTarget }} {
		cfg := DefaultConfig()
		edit(&cfg)
		if _, err := New(cfg, RealClock{}); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	e, _ := testEngine(t, nil)
	for _, job := range []Job{{Tenant: "", MaxOutputTokens: 1}, {Tenant: "a", MaxOutputTokens: 0}, {Tenant: "a", MaxOutputTokens: 65537}, {Tenant: "a", PromptTokens: -1, MaxOutputTokens: 1}} {
		job.Deadline = e.clock.Now().Add(time.Second)
		if _, err := e.Enqueue(job); ReasonOf(err) != Invalid {
			t.Fatalf("invalid request accepted: %+v (%v)", job, err)
		}
	}
}
func BenchmarkPolicyRoundTrip(b *testing.B) {
	cfg := DefaultConfig()
	clock := &ManualClock{Time: time.Unix(1, 0)}
	e, _ := New(cfg, clock)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j, err := e.Enqueue(Job{Tenant: "tenant", PromptTokens: 16, MaxOutputTokens: 128, Deadline: clock.Now().Add(time.Second)})
		if err != nil {
			b.Fatal(err)
		}
		e.Next()
		clock.Set(clock.Now().Add(time.Millisecond))
		e.Complete(j.ID, 64, true)
	}
}

func TestExpensiveHeadAccumulatesCredit(t *testing.T) {
	e, _ := testEngine(t, func(c *Config) { c.Quantum = 1; c.InitialOutput = float64(c.MaxTokens) })
	job := enqueue(t, e, "expensive", 65536)
	dispatched, drops := e.Next()
	if dispatched == nil || dispatched.ID != job.ID || len(drops) > 0 {
		t.Fatal("expensive request failed to accumulate credit")
	}
}

func TestRejectedArrivalPreservesExistingPredictor(t *testing.T) {
	e, clock := testEngine(t, func(c *Config) { c.MaxTenants = 1; c.Alpha = 1 })
	enqueue(t, e, "trained", 512)
	j, _ := e.Next()
	clock.Set(clock.Now().Add(time.Millisecond))
	e.Complete(j.ID, 400, true)
	_, err := e.Enqueue(Job{Tenant: "rejected", MaxOutputTokens: 512, Deadline: clock.Now().Add(time.Nanosecond)})
	if ReasonOf(err) != Deadline {
		t.Fatal(err)
	}
	next := enqueue(t, e, "trained", 512)
	if next.Estimate != 400 {
		t.Fatalf("rejected tenant evicted predictor: %g", next.Estimate)
	}
}
