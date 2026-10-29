// Package policy implements the shared, single-owner scheduling state machine.
// Callers serialize access. No goroutines, network calls, or sleeps occur here.
package policy

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"time"
)

type Reason string

const (
	Invalid      Reason = "invalid"
	QueueFull    Reason = "queue_full"
	TenantLimit  Reason = "tenant_limit"
	Deadline     Reason = "deadline"
	Shed         Reason = "codel"
	Canceled     Reason = "canceled"
	Closed       Reason = "closed"
	BackendError Reason = "backend_error"
)

type Rejection struct{ Reason Reason }

func (e *Rejection) Error() string { return "vessel: " + string(e.Reason) }
func Reject(reason Reason) error   { return &Rejection{Reason: reason} }
func ReasonOf(err error) Reason {
	var r *Rejection
	if errors.As(err, &r) {
		return r.Reason
	}
	return BackendError
}

type Config struct {
	Concurrency    int
	MaxQueue       int
	MaxTenantQueue int
	MaxTenants     int
	MaxTokens      int
	Quantum        float64
	InitialOutput  float64
	Alpha          float64
	TokenTime      time.Duration // Initial service time per token, per execution slot.
	SafetyFactor   float64
	CoDelTarget    time.Duration
	CoDelInterval  time.Duration
	Weights        map[string]int
	Baseline       bool // FIFO concurrency limiter; disables prediction-based admission and CoDel.
}

func DefaultConfig() Config {
	return Config{Concurrency: 8, MaxQueue: 1024, MaxTenantQueue: 256, MaxTenants: 128,
		MaxTokens: 65536, Quantum: 256, InitialOutput: 128, Alpha: 0.2,
		TokenTime: time.Millisecond, SafetyFactor: 1.25,
		CoDelTarget: 50 * time.Millisecond, CoDelInterval: 100 * time.Millisecond}
}
func (c Config) Validate() error {
	if c.Concurrency < 1 || c.MaxQueue < 1 || c.MaxTenantQueue < 1 || c.MaxTenants < 1 || c.MaxTokens < 1 {
		return fmt.Errorf("concurrency, queue, tenant and token limits must be positive")
	}
	if math.IsNaN(c.Quantum) || math.IsInf(c.Quantum, 0) || c.Quantum < 1 || c.Quantum > float64(c.MaxTokens) || math.IsNaN(c.InitialOutput) || math.IsInf(c.InitialOutput, 0) || c.InitialOutput < 1 || c.InitialOutput > float64(c.MaxTokens) || !(c.Alpha > 0 && c.Alpha <= 1) || math.IsNaN(c.SafetyFactor) || math.IsInf(c.SafetyFactor, 0) || c.SafetyFactor < 1 || c.SafetyFactor > 100 || c.TokenTime <= 0 || c.CoDelTarget <= 0 || c.CoDelInterval <= c.CoDelTarget {
		return fmt.Errorf("invalid predictor, service-time, or CoDel configuration")
	}
	for tenant, w := range c.Weights {
		if tenant == "" || len(tenant) > 128 || w < 1 || w > 1000 {
			return fmt.Errorf("tenant weights must be in [1,1000]")
		}
	}
	return nil
}

type Job struct {
	ID              uint64
	Tenant          string
	PromptTokens    int
	MaxOutputTokens int
	Deadline        time.Time
	Enqueued        time.Time
	Estimate        float64
}
type Drop struct {
	Job    *Job
	Reason Reason
}
type tenant struct {
	name            string
	queue           []*Job
	deficit, output float64
	weight          int
	round           bool
	element         *list.Element
	control         codel
	lastUsed        time.Time
	running         int
}
type execution struct {
	job     *Job
	started time.Time
}
type Snapshot struct {
	Queued, Running, Tenants int
	TokenTimeSeconds         float64
}
type Engine struct {
	cfg          Config
	clock        Clock
	tenants      map[string]*tenant
	active       list.List
	running      map[uint64]execution
	runningOrder []uint64
	queued       int
	queuedWork   float64
	tokenSeconds float64
	nextID       uint64
	fifo         []*Job
}

func New(cfg Config, clock Clock) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		return nil, fmt.Errorf("clock is required")
	}
	// Copy caller-owned maps to keep configuration immutable.
	weights := make(map[string]int, len(cfg.Weights))
	for k, v := range cfg.Weights {
		weights[k] = v
	}
	cfg.Weights = weights
	return &Engine{cfg: cfg, clock: clock, tenants: make(map[string]*tenant), running: make(map[uint64]execution), tokenSeconds: cfg.TokenTime.Seconds()}, nil
}
func (e *Engine) Snapshot() Snapshot {
	return Snapshot{e.queued, len(e.running), len(e.tenants), e.tokenSeconds}
}
func (e *Engine) tenantFor(name string) (*tenant, error) {
	if t := e.tenants[name]; t != nil {
		return t, nil
	}
	if len(e.tenants) >= e.cfg.MaxTenants {
		// Evict only idle predictor state; stable tie-breaks make replay deterministic.
		var oldest *tenant
		for _, t := range e.tenants {
			if len(t.queue) == 0 && t.running == 0 && (oldest == nil || t.lastUsed.Before(oldest.lastUsed) || (t.lastUsed.Equal(oldest.lastUsed) && t.name < oldest.name)) {
				oldest = t
			}
		}
		if oldest == nil {
			return nil, Reject(TenantLimit)
		}
		delete(e.tenants, oldest.name)
	}
	weight := e.cfg.Weights[name]
	if weight == 0 {
		weight = 1
	}
	t := &tenant{name: name, weight: weight, output: e.cfg.InitialOutput, lastUsed: e.clock.Now()}
	e.tenants[name] = t
	return t, nil
}
func (e *Engine) Enqueue(j Job) (*Job, error) {
	now := e.clock.Now()
	if j.Tenant == "" || len(j.Tenant) > 128 || j.PromptTokens < 0 || j.MaxOutputTokens < 1 || j.PromptTokens > e.cfg.MaxTokens || j.MaxOutputTokens > e.cfg.MaxTokens-j.PromptTokens || j.Deadline.IsZero() {
		return nil, Reject(Invalid)
	}
	if !j.Deadline.After(now) {
		return nil, Reject(Deadline)
	}
	if e.queued >= e.cfg.MaxQueue {
		return nil, Reject(QueueFull)
	}
	t := e.tenants[j.Tenant]
	if t != nil && len(t.queue) >= e.cfg.MaxTenantQueue {
		return nil, Reject(QueueFull)
	}
	output := e.cfg.InitialOutput
	if t != nil {
		output = t.output
	}
	j.Estimate = float64(j.PromptTokens) + math.Min(output, float64(j.MaxOutputTokens))
	if !e.cfg.Baseline {
		remaining := e.queuedWork
		// Stable accumulation order avoids map-iteration-dependent floating point
		// rounding changing a borderline admission decision during replay.
		for _, id := range e.runningOrder {
			r := e.running[id]
			remaining += math.Max(0, r.job.Estimate-now.Sub(r.started).Seconds()/e.tokenSeconds)
		}
		seconds := (remaining/float64(e.cfg.Concurrency) + j.Estimate) * e.tokenSeconds * e.cfg.SafetyFactor
		if seconds >= j.Deadline.Sub(now).Seconds() {
			return nil, Reject(Deadline)
		}
	}
	if t == nil {
		var err error
		t, err = e.tenantFor(j.Tenant)
		if err != nil {
			return nil, err
		}
	}
	e.nextID++
	j.ID = e.nextID
	j.Enqueued = now
	t.queue = append(t.queue, &j)
	t.lastUsed = now
	if t.element == nil {
		t.element = e.active.PushBack(t)
	}
	if e.cfg.Baseline {
		e.fifo = append(e.fifo, &j)
	}
	e.queued++
	e.queuedWork += j.Estimate
	return &j, nil
}
func (e *Engine) pop(t *tenant) *Job {
	j := t.queue[0]
	t.queue[0] = nil
	t.queue = t.queue[1:]
	e.queued--
	e.queuedWork = math.Max(0, e.queuedWork-j.Estimate)
	if len(t.queue) == 0 {
		e.active.Remove(t.element)
		t.element = nil
		t.queue = nil
		t.deficit = 0
		t.round = false
		t.control = codel{}
	}
	return j
}

// Next returns a dispatch and any discarded requests encountered along the way.
// A nil job means all slots are occupied or the queue is empty.
func (e *Engine) Next() (*Job, []Drop) {
	if len(e.running) >= e.cfg.Concurrency {
		return nil, nil
	}
	now := e.clock.Now()
	var drops []Drop
	rotations := 0
	for e.queued > 0 {
		var t *tenant
		if e.cfg.Baseline {
			t = e.tenants[e.fifo[0].Tenant]
		} else {
			t = e.active.Front().Value.(*tenant)
		}
		j := t.queue[0]
		if !j.Deadline.After(now) {
			e.pop(t)
			if e.cfg.Baseline {
				e.fifo[0] = nil
				e.fifo = e.fifo[1:]
			}
			drops = append(drops, Drop{j, Deadline})
			rotations = 0
			continue
		}
		if !e.cfg.Baseline {
			if t.control.drop(now, now.Sub(j.Enqueued), e.cfg.CoDelTarget, e.cfg.CoDelInterval, e.queued > 1) {
				e.pop(t)
				drops = append(drops, Drop{j, Shed})
				rotations = 0
				continue
			}
			if !t.round {
				t.deficit += e.cfg.Quantum * float64(t.weight)
				t.round = true
			}
			if j.Estimate > t.deficit {
				t.round = false
				e.active.MoveToBack(t.element)
				rotations++
				if rotations >= e.active.Len() {
					// No head fits after a complete pass. Advance whole empty
					// rounds together instead of spinning once per token-unit.
					// The next regular pass preserves tenant visitation order.
					skipped := math.Inf(1)
					for node := e.active.Front(); node != nil; node = node.Next() {
						candidate := node.Value.(*tenant)
						quantum := e.cfg.Quantum * float64(candidate.weight)
						rounds := math.Ceil((candidate.queue[0].Estimate-candidate.deficit)/quantum) - 1
						skipped = math.Min(skipped, rounds)
					}
					if skipped > 0 {
						for node := e.active.Front(); node != nil; node = node.Next() {
							candidate := node.Value.(*tenant)
							candidate.deficit += skipped * e.cfg.Quantum * float64(candidate.weight)
						}
					}
					rotations = 0
				}
				continue
			}
			t.deficit -= j.Estimate
		} else {
			e.fifo[0] = nil
			e.fifo = e.fifo[1:]
		}
		e.pop(t)
		t.running++
		t.lastUsed = now
		e.running[j.ID] = execution{j, now}
		e.runningOrder = append(e.runningOrder, j.ID)
		return j, drops
	}
	return nil, drops
}

// Complete updates the predictor after a successful response. Canceled and
// failed calls are excluded because their output lengths may be incomplete.
func (e *Engine) Complete(id uint64, output int, success bool) bool {
	r, ok := e.running[id]
	if !ok {
		return false
	}
	delete(e.running, id)
	for i, runningID := range e.runningOrder {
		if runningID == id {
			copy(e.runningOrder[i:], e.runningOrder[i+1:])
			e.runningOrder = e.runningOrder[:len(e.runningOrder)-1]
			break
		}
	}
	t := e.tenants[r.job.Tenant]
	t.running--
	t.lastUsed = e.clock.Now()
	if !success || output < 0 || output > r.job.MaxOutputTokens {
		return true
	}
	actual := float64(r.job.PromptTokens + output)
	t.output = (1-e.cfg.Alpha)*t.output + e.cfg.Alpha*math.Max(1, float64(output))
	elapsed := e.clock.Now().Sub(r.started).Seconds()
	if elapsed > 0 && actual > 0 {
		e.tokenSeconds = (1-e.cfg.Alpha)*e.tokenSeconds + e.cfg.Alpha*elapsed/actual
	}
	// Reconcile prediction error while the tenant remains backlogged. Inactive
	// tenants cannot bank credits, matching DRR's empty-queue reset rule.
	if len(t.queue) > 0 {
		t.deficit -= actual - r.job.Estimate
		t.deficit = math.Max(-float64(e.cfg.MaxTokens), t.deficit)
	}
	return true
}
func (e *Engine) Cancel(id uint64) *Job {
	for _, t := range e.tenants {
		for i, j := range t.queue {
			if j.ID == id {
				copy(t.queue[i:], t.queue[i+1:])
				t.queue[len(t.queue)-1] = nil
				t.queue = t.queue[:len(t.queue)-1]
				e.queued--
				e.queuedWork = math.Max(0, e.queuedWork-j.Estimate)
				if len(t.queue) == 0 {
					e.active.Remove(t.element)
					t.element = nil
					t.queue = nil
					t.deficit = 0
					t.round = false
					t.control = codel{}
				}
				if e.cfg.Baseline {
					for k, f := range e.fifo {
						if f.ID == id {
							copy(e.fifo[k:], e.fifo[k+1:])
							e.fifo[len(e.fifo)-1] = nil
							e.fifo = e.fifo[:len(e.fifo)-1]
							break
						}
					}
				}
				return j
			}
		}
	}
	return nil
}
