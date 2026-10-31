package gateway

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/theruknology/govessel/internal/inference"
	"github.com/theruknology/govessel/internal/policy"
)

type backendFunc func(context.Context, inference.Request) (inference.Result, error)

func (f backendFunc) Infer(ctx context.Context, r inference.Request) (inference.Result, error) {
	return f(ctx, r)
}
func config() policy.Config {
	c := policy.DefaultConfig()
	c.Concurrency = 1
	c.InitialOutput = 8
	c.TokenTime = time.Microsecond
	c.CoDelTarget = time.Hour
	c.CoDelInterval = 2 * time.Hour
	return c
}
func eventually(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		runtime.Gosched()
	}
}
func cleanup(t *testing.T, g *Gateway) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := g.Close(ctx); err != nil {
			t.Error(err)
		}
	})
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for result")
		var zero T
		return zero
	}
}
func TestConcurrentCapAndDrain(t *testing.T) {
	cfg := config()
	cfg.Concurrency = 3
	release := make(chan struct{})
	var active, max atomic.Int32
	backend := backendFunc(func(ctx context.Context, r inference.Request) (inference.Result, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for prev := max.Load(); n > prev; prev = max.Load() {
			if max.CompareAndSwap(prev, n) {
				break
			}
		}
		select {
		case <-release:
			return inference.Result{OutputTokens: 8}, nil
		case <-ctx.Done():
			return inference.Result{}, ctx.Err()
		}
	})
	g, err := New(cfg, backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	cleanup(t, g)
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := g.Infer(ctx, inference.Request{Tenant: "a", Prompt: "test", MaxOutputTokens: 16})
			errs <- err
		}()
	}
	eventually(t, func() bool { s := g.Snapshot(); return s.Running == 3 && s.Queued == 97 })
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if max.Load() != 3 {
		t.Fatalf("peak concurrency %d", max.Load())
	}
	if s := g.Snapshot(); s.Running != 0 || s.Queued != 0 {
		t.Fatal(s)
	}
}
func TestCanceledBackendRetainsSlot(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	backend := backendFunc(func(_ context.Context, _ inference.Request) (inference.Result, error) {
		started <- struct{}{}
		<-release
		return inference.Result{OutputTokens: 8}, nil
	})
	reg := prometheus.NewRegistry()
	g, err := New(config(), backend, NewMetrics(reg))
	if err != nil {
		t.Fatal(err)
	}
	cleanup(t, g)
	t.Cleanup(func() { close(release) })
	first, cancelFirst := context.WithCancel(context.Background())
	first, deadlineCancel := context.WithTimeout(first, 5*time.Second)
	defer deadlineCancel()
	result := make(chan error, 1)
	go func() { _, err := g.Infer(first, inference.Request{Tenant: "a", MaxOutputTokens: 16}); result <- err }()
	receive(t, started)
	cancelFirst()
	if err := receive(t, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	second, cancelSecond := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelSecond()
	go func() { _, err := g.Infer(second, inference.Request{Tenant: "b", MaxOutputTokens: 16}); result <- err }()
	eventually(t, func() bool { s := g.Snapshot(); return s.Running == 1 && s.Queued == 1 })
	select {
	case <-started:
		t.Fatal("canceled backend released its slot before returning")
	default:
	}
	release <- struct{}{}
	receive(t, started)
	release <- struct{}{}
	if err := receive(t, result); err != nil {
		t.Fatal(err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var count float64
	for _, family := range families {
		if family.GetName() == "vessel_requests_total" {
			for _, metric := range family.Metric {
				count += metric.GetCounter().GetValue()
			}
		}
	}
	if count != 2 {
		t.Fatalf("expected exactly two terminal metrics, got %g", count)
	}
}
func TestQueuedCancellationAndShutdown(t *testing.T) {
	started := make(chan struct{}, 1)
	backend := backendFunc(func(ctx context.Context, _ inference.Request) (inference.Result, error) {
		started <- struct{}{}
		<-ctx.Done()
		return inference.Result{}, ctx.Err()
	})
	g, err := New(config(), backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	cleanup(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := g.Infer(ctx, inference.Request{Tenant: "a", MaxOutputTokens: 16}); first <- err }()
	receive(t, started)
	queued, stop := context.WithCancel(ctx)
	second := make(chan error, 1)
	go func() { _, err := g.Infer(queued, inference.Request{Tenant: "b", MaxOutputTokens: 16}); second <- err }()
	eventually(t, func() bool { return g.Snapshot().Queued == 1 })
	stop()
	if err := receive(t, second); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	eventually(t, func() bool { return g.Snapshot().Queued == 0 })
	if err := g.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, first); policy.ReasonOf(err) != policy.Closed {
		t.Fatal(err)
	}
	if _, err := g.Infer(ctx, inference.Request{Tenant: "a", MaxOutputTokens: 16}); policy.ReasonOf(err) != policy.Closed {
		t.Fatal("closed gateway accepted work")
	}
}
func TestBackendPanicReleasesSlot(t *testing.T) {
	g, err := New(config(), backendFunc(func(context.Context, inference.Request) (inference.Result, error) { panic("bad backend") }), nil)
	if err != nil {
		t.Fatal(err)
	}
	cleanup(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if _, err := g.Infer(ctx, inference.Request{Tenant: "a", MaxOutputTokens: 16}); err == nil {
			t.Fatal("panic lost")
		}
	}
	if g.Snapshot().Running != 0 {
		t.Fatal("slot leaked")
	}
}

// This measures the in-process gateway path around an immediate backend. It is
// a microbenchmark; it does not establish HTTP/gRPC p99 at a fixed offered load.
func BenchmarkGatewayImmediateBackend(b *testing.B) {
	cfg := config()
	g, err := New(cfg, backendFunc(func(context.Context, inference.Request) (inference.Result, error) {
		return inference.Result{OutputTokens: 8}, nil
	}), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = g.Close(context.Background()) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.Infer(ctx, inference.Request{Tenant: "tenant", Prompt: "test", MaxOutputTokens: 16}); err != nil {
			b.Fatal(err)
		}
	}
}
