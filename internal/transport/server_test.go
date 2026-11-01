package transport

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	vesselv1 "github.com/theruknology/govessel/api/vessel/v1"
	"github.com/theruknology/govessel/internal/gateway"
	"github.com/theruknology/govessel/internal/inference"
	"github.com/theruknology/govessel/internal/policy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type echoBackend struct{}

func (echoBackend) Infer(ctx context.Context, r inference.Request) (inference.Result, error) {
	return inference.Result{Text: r.Prompt, OutputTokens: 1}, nil
}
func fixture(t *testing.T) (*Server, *prometheus.Registry) {
	t.Helper()
	cfg := policy.DefaultConfig()
	cfg.TokenTime = time.Microsecond
	reg := prometheus.NewRegistry()
	g, err := gateway.New(cfg, echoBackend{}, gateway.NewMetrics(reg))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := g.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return &Server{Gateway: g, Timeout: time.Second}, reg
}
func TestHTTPAndMetrics(t *testing.T) {
	s, reg := fixture(t)
	handler := s.Handler(reg)
	for _, test := range []struct {
		body string
		code int
	}{
		{`{"tenant_id":"a","prompt":"hello","max_output_tokens":16}`, 200},
		{`{"tenant_id":"a","prompt":"hello","max_output_tokens":16,"unknown":true}`, 400},
		{`{"tenant_id":"a","max_output_tokens":16} {}`, 400},
		{`{"max_output_tokens":16}`, 400},
		{`{"tenant_id":"a","max_output_tokens":16,"timeout_ms":1001}`, 400},
		{`{"tenant_id":"a","max_output_tokens":16,"timeout_ms":-1}`, 400},
	} {
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/infer", strings.NewReader(test.body)))
		if rr.Code != test.code {
			t.Fatalf("%s: HTTP %d: %s", test.body, rr.Code, rr.Body)
		}
		if rr.Code == 200 {
			var r inference.Result
			if err := json.Unmarshal(rr.Body.Bytes(), &r); err != nil || r.Text != "hello" || r.RequestID == 0 {
				t.Fatalf("invalid response: %+v %v", r, err)
			}
		}
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), `vessel_requests_total{outcome="completed"} 1`) {
		t.Fatal(rr.Body.String())
	}
}
func TestGRPCRoundTripAndDeadlineStatus(t *testing.T) {
	s, _ := fixture(t)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	vesselv1.RegisterInferenceServiceServer(server, s)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := vesselv1.NewInferenceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := client.Infer(ctx, &vesselv1.InferRequest{TenantId: "a", Prompt: "hello", MaxOutputTokens: 16})
	if err != nil || result.GetText() != "hello" || result.GetRequestId() == 0 {
		t.Fatalf("%+v %v", result, err)
	}
	_, err = client.Infer(ctx, &vesselv1.InferRequest{MaxOutputTokens: 16})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	_, err = client.Infer(expired, &vesselv1.InferRequest{TenantId: "a", MaxOutputTokens: 16})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
}
