// bench measures the in-process gateway around an immediate synthetic backend.
// HTTP/gRPC serialization and model execution are outside the measurement.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/theruknology/govessel/internal/gateway"
	"github.com/theruknology/govessel/internal/inference"
	"github.com/theruknology/govessel/internal/policy"
)

type timingKey struct{}
type sample struct {
	gateway, arrival time.Duration
	failed           bool
}
type immediate struct{}

func (immediate) Infer(ctx context.Context, _ inference.Request) (inference.Result, error) {
	started := time.Now()
	result := inference.Result{Text: "benchmark", OutputTokens: 64}
	*ctx.Value(timingKey{}).(*time.Duration) = time.Since(started)
	return result, nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	rps := flag.Int("rps", 2000, "offered requests per second")
	duration := flag.Duration("duration", 5*time.Second, "offered-load duration")
	flag.Parse()
	if *rps < 1 || *rps > 100000 || *duration <= 0 || duration.Seconds()*float64(*rps) > 1000000 {
		return fmt.Errorf("require rps in [1,100000] and 1 to 1,000,000 total requests")
	}
	n := int(duration.Seconds() * float64(*rps))
	if n < 1 {
		return fmt.Errorf("duration too short")
	}
	cfg := policy.DefaultConfig()
	cfg.TokenTime = time.Microsecond
	g, err := gateway.New(cfg, immediate{}, gateway.NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		return err
	}
	defer func() { _ = g.Close(context.Background()) }()
	request := inference.Request{Tenant: "benchmark", Prompt: "benchmark prompt", MaxOutputTokens: 128}
	call := func(scheduled time.Time) sample {
		var backendTime time.Duration
		ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), timingKey{}, &backendTime), time.Second)
		defer cancel()
		started := time.Now()
		_, err := g.Infer(ctx, request)
		finished := time.Now()
		var overhead time.Duration
		if err == nil {
			overhead = finished.Sub(started) - backendTime
		}
		if overhead < 0 {
			overhead = 0
		}
		return sample{overhead, finished.Sub(scheduled), err != nil}
	}
	for i := 0; i < 100; i++ {
		if call(time.Now()).failed {
			return fmt.Errorf("warmup failed")
		}
	}
	samples := make([]sample, n)
	start := time.Now().Add(10 * time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		scheduled := start.Add(time.Duration(i) * time.Second / time.Duration(*rps))
		if wait := time.Until(scheduled); wait > 0 {
			time.Sleep(wait)
		}
		wg.Add(1)
		go func(index int, scheduled time.Time) { defer wg.Done(); samples[index] = call(scheduled) }(i, scheduled)
	}
	wg.Wait()
	elapsed := time.Since(start)
	gatewayTimes := make([]float64, 0, n)
	arrivalTimes := make([]float64, 0, n)
	failed := 0
	for _, s := range samples {
		if s.failed {
			failed++
			continue
		}
		gatewayTimes = append(gatewayTimes, float64(s.gateway)/float64(time.Microsecond))
		arrivalTimes = append(arrivalTimes, float64(s.arrival)/float64(time.Millisecond))
	}
	sort.Float64s(gatewayTimes)
	sort.Float64s(arrivalTimes)
	percentile := func(values []float64, p float64) float64 {
		if len(values) == 0 {
			return 0
		}
		index := int(math.Ceil(float64(len(values))*p)) - 1
		if index >= len(values) {
			index = len(values) - 1
		}
		return values[index]
	}
	report := struct {
		Scope                 string  `json:"scope"`
		OfferedRPS            int     `json:"offered_rps"`
		Requests              int     `json:"requests"`
		Failed                int     `json:"failed"`
		ElapsedSeconds        float64 `json:"elapsed_seconds"`
		GatewayP50US          float64 `json:"gateway_p50_us"`
		GatewayP99US          float64 `json:"gateway_p99_us"`
		ScheduledArrivalP99MS float64 `json:"scheduled_arrival_p99_ms"`
	}{"in-process gateway with Prometheus metrics and immediate backend; excludes network and model", *rps, n, failed, elapsed.Seconds(), percentile(gatewayTimes, 0.5), percentile(gatewayTimes, 0.99), percentile(arrivalTimes, 0.99)}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		return err
	}
	if failed > 0 {
		return fmt.Errorf("%d requests failed; inspect results before comparing latency", failed)
	}
	return nil
}
