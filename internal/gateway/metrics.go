package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/theruknology/govessel/internal/policy"
	"time"
)

type Metrics struct {
	outcomes        *prometheus.CounterVec
	latency, queue  prometheus.Histogram
	queued, running prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		outcomes: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "vessel_requests_total", Help: "Terminal requests by bounded outcome, including admission rejections."}, []string{"outcome"}),
		latency:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "vessel_request_duration_seconds", Help: "Arrival to terminal outcome for all requests, including rejections.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 16)}),
		queue:    prometheus.NewHistogram(prometheus.HistogramOpts{Name: "vessel_queue_duration_seconds", Help: "Queue wait for dispatched requests.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 14)}),
		queued:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "vessel_queued_requests", Help: "Requests awaiting dispatch."}),
		running:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "vessel_running_requests", Help: "Occupied backend slots, including canceling calls."}),
	}
	reg.MustRegister(m.outcomes, m.latency, m.queue, m.queued, m.running)
	return m
}
func (m *Metrics) outcome(reason string, elapsed time.Duration) {
	if m != nil {
		m.outcomes.WithLabelValues(reason).Inc()
		m.latency.Observe(elapsed.Seconds())
	}
}
func (m *Metrics) state(s policy.Snapshot) {
	if m != nil {
		m.queued.Set(float64(s.Queued))
		m.running.Set(float64(s.Running))
	}
}
