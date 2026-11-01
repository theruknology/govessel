package transport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	vesselv1 "github.com/theruknology/govessel/api/vessel/v1"
	"github.com/theruknology/govessel/internal/gateway"
	"github.com/theruknology/govessel/internal/inference"
	"github.com/theruknology/govessel/internal/policy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	vesselv1.UnimplementedInferenceServiceServer
	Gateway *gateway.Gateway
	Timeout time.Duration
}

func (s *Server) Infer(ctx context.Context, r *vesselv1.InferRequest) (*vesselv1.InferResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()
	result, err := s.Gateway.Infer(ctx, inference.Request{Tenant: r.GetTenantId(), Prompt: r.GetPrompt(), MaxOutputTokens: int(r.GetMaxOutputTokens())})
	if err != nil {
		code := grpcCode(err)
		return nil, status.Error(code, safeMessage(err))
	}
	return &vesselv1.InferResponse{RequestId: result.RequestID, Text: result.Text, OutputTokens: int32(result.OutputTokens)}, nil
}
func safeMessage(err error) string {
	if errors.Is(err, context.Canceled) {
		return "request canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "request deadline exceeded"
	}
	reason := policy.ReasonOf(err)
	if reason == policy.BackendError {
		return "inference backend failed"
	}
	return "vessel: " + string(reason)
}
func grpcCode(err error) codes.Code {
	if errors.Is(err, context.Canceled) {
		return codes.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return codes.DeadlineExceeded
	}
	switch policy.ReasonOf(err) {
	case policy.Invalid:
		return codes.InvalidArgument
	case policy.Deadline:
		return codes.DeadlineExceeded
	case policy.QueueFull, policy.TenantLimit, policy.Shed:
		return codes.ResourceExhausted
	case policy.Closed:
		return codes.Unavailable
	default:
		return codes.Unavailable
	}
}
func (s *Server) Handler(reg prometheus.Gatherer) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{\"status\":\"ok\"}\n")
	})
	mux.HandleFunc("POST /v1/infer", s.httpInfer)
	return mux
}
func (s *Server) httpInfer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var request struct {
		inference.Request
		TimeoutMS int64 `json:"timeout_ms"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request JSON")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "expected one JSON object")
		return
	}
	if request.TimeoutMS < 0 || request.TimeoutMS > int64(s.Timeout/time.Millisecond) {
		writeError(w, http.StatusBadRequest, "timeout_ms exceeds server limit or is negative")
		return
	}
	timeout := s.Timeout
	if request.TimeoutMS > 0 {
		timeout = time.Duration(request.TimeoutMS) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	result, err := s.Gateway.Infer(ctx, request.Request)
	if err != nil {
		code := http.StatusServiceUnavailable
		switch grpcCode(err) {
		case codes.InvalidArgument:
			code = http.StatusBadRequest
		case codes.DeadlineExceeded:
			code = http.StatusGatewayTimeout
		case codes.Canceled:
			code = 499
		case codes.ResourceExhausted:
			code = http.StatusTooManyRequests
			w.Header().Set("Retry-After", "1")
		}
		writeError(w, code, safeMessage(err))
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}
func writeError(w http.ResponseWriter, code int, message string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
