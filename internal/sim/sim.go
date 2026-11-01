// Package sim runs seeded workloads against the scheduler using virtual time.
package sim

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/theruknology/govessel/internal/policy"
)

var Scenarios = []string{"overload", "burst", "noisy_neighbor", "output_shift", "backend_slowdown", "backend_errors", "cancellations", "deadline_crunch"}

type Work struct {
	At                           time.Duration
	Tenant                       string
	Prompt, Output, MaxOutput    int
	Budget, Service, CancelAfter time.Duration
	Fail                         bool
}

// Workload fixes arrivals and backend outcomes for a scenario and seed.
func Workload(scenario string, seed int64, duration time.Duration) ([]Work, error) {
	valid := false
	for _, s := range Scenarios {
		if scenario == s {
			valid = true
		}
	}
	if !valid || duration <= 0 {
		return nil, fmt.Errorf("unknown scenario or invalid duration")
	}
	rng := rand.New(rand.NewSource(seed))
	var work []Work
	tenantNames := [...]string{"tenant-0", "tenant-1", "tenant-2", "tenant-3"}
	rate := 35.0
	if scenario == "overload" {
		rate = 150
	} // 3x nominal capacity: 4 slots / 80ms mean service.
	for at := time.Duration(0); at < duration; {
		interval := time.Duration(rng.ExpFloat64() / rate * float64(time.Second))
		if interval < time.Microsecond {
			interval = time.Microsecond
		}
		at += interval
		if at >= duration {
			break
		}
		if scenario == "burst" && at%(4*time.Second) < time.Second {
			at -= interval
			interval /= 6
			if interval < time.Microsecond {
				interval = time.Microsecond
			}
			at += interval
		}
		output := 32 + rng.Intn(65)
		tenant := tenantNames[rng.Intn(len(tenantNames))]
		budget := 1500 * time.Millisecond
		maxOutput := 256
		if scenario == "noisy_neighbor" && rng.Float64() < 0.7 {
			tenant = "heavy"
			output = 512 + rng.Intn(257)
			maxOutput = 1024
		}
		if scenario == "output_shift" && at > duration/2 {
			output *= 5
			maxOutput = 1024
		}
		service := time.Duration(16+output) * time.Millisecond
		if scenario == "backend_slowdown" && at > duration/3 && at < 2*duration/3 {
			service *= 8
		}
		fail := scenario == "backend_errors" && rng.Float64() < 0.3
		cancel := time.Duration(0)
		if scenario == "cancellations" && rng.Float64() < 0.35 {
			cancel = time.Duration(10+rng.Intn(80)) * time.Millisecond
		}
		if scenario == "deadline_crunch" && rng.Float64() < 0.65 {
			budget = time.Duration(15+rng.Intn(75)) * time.Millisecond
		}
		work = append(work, Work{at, tenant, 16, output, maxOutput, budget, service, cancel, fail})
	}
	return work, nil
}

type Report struct {
	Scenario             string         `json:"scenario"`
	Seed                 int64          `json:"seed"`
	Policy               string         `json:"policy"`
	Offered              int            `json:"offered"`
	Admitted             int            `json:"admitted"`
	Completed            int            `json:"completed"`
	Timely               int            `json:"timely"`
	Rejected             int            `json:"rejected"`
	Shed                 int            `json:"shed"`
	Expired              int            `json:"expired"`
	Canceled             int            `json:"canceled"`
	Failed               int            `json:"failed"`
	PeakQueue            int            `json:"peak_queue"`
	PeakRunning          int            `json:"peak_running"`
	TimelyOffered        float64        `json:"timely_per_offered"`
	TimelyAdmitted       float64        `json:"timely_per_admitted"`
	CompletedP99MS       float64        `json:"completed_p99_ms"`
	TerminalP99MS        float64        `json:"all_terminal_p99_ms"`
	WastedServiceSeconds float64        `json:"wasted_service_seconds"`
	TokensByTenant       map[string]int `json:"completed_tokens_by_tenant"`
}
type event struct {
	at    time.Duration
	kind  int
	index int
	seq   uint64
}
type events []event

func (h events) Len() int { return len(h) }
func (h events) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	if h[i].kind != h[j].kind {
		return h[i].kind < h[j].kind
	}
	return h[i].seq < h[j].seq
}
func (h events) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *events) Push(x any)   { *h = append(*h, x.(event)) }
func (h *events) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

const (
	completion   = 0
	expiration   = 1
	cancellation = 2
	arrival      = 3
)

type state struct {
	job           *policy.Job
	done, running bool
	started       time.Duration
}

func Run(scenario string, seed int64, work []Work, baseline bool) (Report, error) {
	cfg := policy.DefaultConfig()
	cfg.Concurrency = 4
	cfg.InitialOutput = 64
	cfg.Baseline = baseline
	origin := time.Unix(0, 0).UTC()
	clock := &policy.ManualClock{Time: origin}
	engine, err := policy.New(cfg, clock)
	if err != nil {
		return Report{}, err
	}
	report := Report{Scenario: scenario, Seed: seed, Policy: "vessel", Offered: len(work), TokensByTenant: make(map[string]int)}
	if baseline {
		report.Policy = "semaphore"
	}
	states := make([]state, len(work))
	byID := make(map[uint64]int)
	h := events{}
	var seq uint64
	push := func(at time.Duration, kind, index int) { seq++; heap.Push(&h, event{at, kind, index, seq}) }
	for i, w := range work {
		if w.At < 0 || w.Budget <= 0 || w.Service <= 0 || w.Output < 0 || w.Output > w.MaxOutput || w.CancelAfter < 0 {
			return Report{}, fmt.Errorf("invalid synthetic work at index %d", i)
		}
		if i > 0 && w.At < work[i-1].At {
			return Report{}, fmt.Errorf("work must be ordered by arrival time")
		}
	}
	var terminal, completed []float64
	finish := func(i int, now time.Duration, reason policy.Reason) {
		s := &states[i]
		if s.done {
			return
		}
		s.done = true
		w := work[i]
		if s.running {
			engine.Complete(s.job.ID, w.Output, reason == "")
			if reason != "" {
				report.WastedServiceSeconds += (now - s.started).Seconds()
			}
		} else if s.job != nil {
			engine.Cancel(s.job.ID)
		}
		if s.job != nil {
			delete(byID, s.job.ID)
		}
		elapsed := float64(now-w.At) / float64(time.Millisecond)
		terminal = append(terminal, elapsed)
		switch reason {
		case "":
			report.Completed++
			completed = append(completed, elapsed)
			report.TokensByTenant[w.Tenant] += w.Prompt + w.Output
			if now <= w.At+w.Budget {
				report.Timely++
			}
		case policy.Shed:
			report.Shed++
		case policy.Deadline:
			report.Expired++
		case policy.Canceled:
			report.Canceled++
		case policy.BackendError:
			report.Failed++
		default:
			report.Rejected++
		}
	}
	// Arrivals are already ordered. Merge them with the heap rather than
	// inserting millions of future arrivals into it. Heap memory and lookup
	// cost then track outstanding work, independent of experiment duration.
	nextArrival := 0
	for h.Len() > 0 || nextArrival < len(work) {
		var ev event
		if nextArrival < len(work) && (h.Len() == 0 || work[nextArrival].At < h[0].at) {
			ev = event{at: work[nextArrival].At, kind: arrival, index: nextArrival}
			nextArrival++
		} else {
			ev = heap.Pop(&h).(event)
		}
		clock.Set(origin.Add(ev.at))
		s := &states[ev.index]
		w := work[ev.index]
		if s.done {
			continue
		}
		switch ev.kind {
		case arrival:
			job, err := engine.Enqueue(policy.Job{Tenant: w.Tenant, PromptTokens: w.Prompt, MaxOutputTokens: w.MaxOutput, Deadline: origin.Add(w.At + w.Budget)})
			if err != nil {
				report.Rejected++
				s.done = true
				terminal = append(terminal, 0)
			} else {
				s.job = job
				byID[job.ID] = ev.index
				report.Admitted++
				push(w.At+w.Budget, expiration, ev.index)
				if w.CancelAfter > 0 {
					push(w.At+w.CancelAfter, cancellation, ev.index)
				}
			}
		case completion:
			reason := policy.Reason("")
			if w.Fail {
				reason = policy.BackendError
			}
			finish(ev.index, ev.at, reason)
		case expiration:
			finish(ev.index, ev.at, policy.Deadline)
		case cancellation:
			finish(ev.index, ev.at, policy.Canceled)
		}
		for {
			job, drops := engine.Next()
			for _, drop := range drops {
				i := byID[drop.Job.ID]
				finish(i, ev.at, drop.Reason)
			}
			if job == nil {
				break
			}
			i := byID[job.ID]
			states[i].running = true
			states[i].started = ev.at
			push(ev.at+work[i].Service, completion, i)
		}
		snap := engine.Snapshot()
		if snap.Queued > report.PeakQueue {
			report.PeakQueue = snap.Queued
		}
		if snap.Running > report.PeakRunning {
			report.PeakRunning = snap.Running
		}
		if snap.Queued > cfg.MaxQueue || snap.Running > cfg.Concurrency || snap.Queued < 0 {
			return report, fmt.Errorf("capacity invariant failed")
		}
	}
	report.CompletedP99MS = percentile(completed, 0.99)
	report.TerminalP99MS = percentile(terminal, 0.99)
	if report.Offered > 0 {
		report.TimelyOffered = float64(report.Timely) / float64(report.Offered)
	}
	if report.Admitted > 0 {
		report.TimelyAdmitted = float64(report.Timely) / float64(report.Admitted)
	}
	if report.Offered != report.Rejected+report.Completed+report.Shed+report.Expired+report.Canceled+report.Failed {
		return report, fmt.Errorf("terminal accounting invariant failed")
	}
	if snap := engine.Snapshot(); snap.Queued != 0 || snap.Running != 0 {
		return report, fmt.Errorf("scheduler did not drain")
	}
	return report, nil
}
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return values[int(math.Ceil(float64(len(values))*p))-1]
}
