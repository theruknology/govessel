package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/theruknology/govessel/internal/inference"
	"github.com/theruknology/govessel/internal/policy"
)

type response struct {
	result inference.Result
	err    error
}
type entry struct {
	job               *policy.Job
	request           inference.Request
	ctx               context.Context
	cancel            context.CancelFunc
	reply             chan response
	started, finished bool
}
type Gateway struct {
	mu      sync.Mutex
	engine  *policy.Engine
	backend inference.Backend
	metrics *Metrics
	entries map[uint64]*entry
	closed  bool
	stop    chan struct{}
	wg      sync.WaitGroup
}

func New(cfg policy.Config, b inference.Backend, m *Metrics) (*Gateway, error) {
	if b == nil {
		return nil, fmt.Errorf("backend is required")
	}
	engine, err := policy.New(cfg, policy.RealClock{})
	if err != nil {
		return nil, err
	}
	g := &Gateway{engine: engine, backend: b, metrics: m, entries: make(map[uint64]*entry), stop: make(chan struct{})}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-g.stop:
				return
			case <-ticker.C:
				g.mu.Lock()
				if !g.closed {
					g.dispatch()
				}
				g.mu.Unlock()
			}
		}
	}()
	return g, nil
}
func (g *Gateway) Infer(ctx context.Context, r inference.Request) (inference.Result, error) {
	arrived := time.Now()
	deadline, ok := ctx.Deadline()
	if !ok {
		return inference.Result{}, policy.Reject(policy.Invalid)
	}
	if err := ctx.Err(); err != nil {
		return inference.Result{}, err
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		g.metrics.outcome(string(policy.Closed), time.Since(arrived))
		return inference.Result{}, policy.Reject(policy.Closed)
	}
	job, err := g.engine.Enqueue(policy.Job{Tenant: r.Tenant, PromptTokens: inference.PromptTokens(r.Prompt), MaxOutputTokens: r.MaxOutputTokens, Deadline: deadline})
	if err != nil {
		g.mu.Unlock()
		g.metrics.outcome(string(policy.ReasonOf(err)), time.Since(arrived))
		return inference.Result{}, err
	}
	callCtx, cancel := context.WithCancel(ctx)
	e := &entry{job: job, request: r, ctx: callCtx, cancel: cancel, reply: make(chan response, 1)}
	g.entries[job.ID] = e
	g.dispatch()
	g.mu.Unlock()
	select {
	case resp := <-e.reply:
		return resp.result, resp.err
	case <-ctx.Done():
		g.mu.Lock()
		if !e.finished {
			g.finish(e, inference.Result{}, ctx.Err())
			if !e.started {
				g.engine.Cancel(job.ID)
				delete(g.entries, job.ID)
			}
			g.dispatch()
		}
		g.mu.Unlock()
		return inference.Result{}, ctx.Err()
	}
}

// finish requires mu. Running calls retain their slots until execute returns,
// including calls whose cancellation has already been reported to the caller.
func (g *Gateway) finish(e *entry, result inference.Result, err error) {
	if e.finished {
		return
	}
	e.finished = true
	e.cancel()
	reason := "completed"
	if errors.Is(err, context.DeadlineExceeded) {
		reason = string(policy.Deadline)
	} else if errors.Is(err, context.Canceled) {
		reason = string(policy.Canceled)
	} else if err != nil {
		reason = string(policy.ReasonOf(err))
	}
	g.metrics.outcome(reason, time.Since(e.job.Enqueued))
	result.RequestID = e.job.ID
	e.reply <- response{result, err}
}
func (g *Gateway) dispatch() {
	if g.closed {
		g.metrics.state(g.engine.Snapshot())
		return
	}
	for {
		job, drops := g.engine.Next()
		for _, drop := range drops {
			e := g.entries[drop.Job.ID]
			g.finish(e, inference.Result{}, policy.Reject(drop.Reason))
			delete(g.entries, drop.Job.ID)
		}
		if job == nil {
			break
		}
		e := g.entries[job.ID]
		e.started = true
		if g.metrics != nil {
			g.metrics.queue.Observe(time.Since(job.Enqueued).Seconds())
		}
		g.wg.Add(1)
		go g.execute(e)
	}
	g.metrics.state(g.engine.Snapshot())
}
func (g *Gateway) execute(e *entry) {
	defer g.wg.Done()
	// Recover here so a backend panic still releases its slot and replies.
	var result inference.Result
	var err error
	func() {
		defer func() {
			if recover() != nil {
				err = fmt.Errorf("backend panic")
			}
		}()
		result, err = g.backend.Infer(e.ctx, e.request)
	}()
	if err == nil && (result.OutputTokens < 0 || result.OutputTokens > e.request.MaxOutputTokens) {
		err = fmt.Errorf("backend returned invalid token count")
	}
	if err == nil {
		err = e.ctx.Err()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.engine.Complete(e.job.ID, result.OutputTokens, err == nil)
	g.finish(e, result, err)
	delete(g.entries, e.job.ID)
	g.dispatch()
}
func (g *Gateway) Snapshot() policy.Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.engine.Snapshot()
}
func (g *Gateway) Close(ctx context.Context) error {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		close(g.stop)
		for id, e := range g.entries {
			g.finish(e, inference.Result{}, policy.Reject(policy.Closed))
			if !e.started {
				g.engine.Cancel(id)
				delete(g.entries, id)
			}
		}
		g.metrics.state(g.engine.Snapshot())
	}
	g.mu.Unlock()
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
