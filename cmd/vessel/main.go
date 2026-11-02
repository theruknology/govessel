package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	vesselv1 "github.com/theruknology/govessel/api/vessel/v1"
	"github.com/theruknology/govessel/internal/backend"
	"github.com/theruknology/govessel/internal/gateway"
	"github.com/theruknology/govessel/internal/inference"
	"github.com/theruknology/govessel/internal/policy"
	"github.com/theruknology/govessel/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if err := run(); err != nil {
		slog.Error("vessel stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	cfg := policy.DefaultConfig()
	httpAddr := flag.String("http", "127.0.0.1:8080", "HTTP and metrics listen address")
	grpcAddr := flag.String("grpc", "127.0.0.1:9090", "gRPC listen address")
	upstream := flag.String("upstream", "", "OpenAI-compatible chat completions URL; empty uses synthetic backend")
	model := flag.String("model", "", "upstream model name")
	timeout := flag.Duration("timeout", 10*time.Second, "maximum request duration")
	flag.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "backend execution slots")
	flag.IntVar(&cfg.MaxQueue, "queue", cfg.MaxQueue, "global queued request limit")
	flag.IntVar(&cfg.MaxTenantQueue, "tenant-queue", cfg.MaxTenantQueue, "per-tenant queued request limit")
	flag.IntVar(&cfg.MaxTenants, "tenants", cfg.MaxTenants, "maximum retained tenant states")
	flag.DurationVar(&cfg.TokenTime, "token-time", cfg.TokenTime, "initial service time per token; also synthetic backend speed")
	flag.DurationVar(&cfg.CoDelTarget, "codel-target", cfg.CoDelTarget, "target queue delay")
	flag.DurationVar(&cfg.CoDelInterval, "codel-interval", cfg.CoDelInterval, "CoDel persistence interval")
	flag.BoolVar(&cfg.Baseline, "baseline", false, "use FIFO semaphore baseline")
	weights := flag.String("weights", "", "JSON map of tenant IDs to integer scheduling weights")
	flag.Parse()
	if *weights != "" {
		if err := parseWeights(*weights, &cfg); err != nil {
			return err
		}
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if *timeout <= 0 || *timeout > 24*time.Hour {
		return fmt.Errorf("timeout must be in (0,24h]")
	}
	var b inference.Backend = backend.Synthetic{TokenTime: cfg.TokenTime}
	if *upstream != "" {
		remote := backend.OpenAI{URL: *upstream, Model: *model, APIKey: os.Getenv("VESSEL_UPSTREAM_API_KEY"), Client: &http.Client{Timeout: *timeout}}
		if err := remote.Validate(); err != nil {
			return err
		}
		b = remote
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	g, err := gateway.New(cfg, b, gateway.NewMetrics(reg))
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = g.Close(ctx)
	}()
	httpListener, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		return err
	}
	defer httpListener.Close()
	grpcListener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return err
	}
	defer grpcListener.Close()
	service := &transport.Server{Gateway: g, Timeout: *timeout}
	httpServer := &http.Server{Handler: service.Handler(reg), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: *timeout + time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(9<<20))
	vesselv1.RegisterInferenceServiceServer(grpcServer, service)
	healthServer := health.NewServer()
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("vessel.v1.InferenceService", healthv1.HealthCheckResponse_SERVING)
	failures := make(chan error, 2)
	go func() { failures <- httpServer.Serve(httpListener) }()
	go func() { failures <- grpcServer.Serve(grpcListener) }()
	slog.Info("vessel listening", "http", httpListener.Addr(), "grpc", grpcListener.Addr(), "synthetic", *upstream == "", "concurrency", cfg.Concurrency, "baseline", cfg.Baseline)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-failures:
	}
	healthServer.Shutdown()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Cancel gateway work first so network shutdown can finish promptly.
	closeErr := g.Close(shutdown)
	grpcServer.Stop()
	httpErr := httpServer.Shutdown(shutdown)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && !errors.Is(serveErr, grpc.ErrServerStopped) {
		return serveErr
	}
	return errors.Join(closeErr, httpErr)
}
