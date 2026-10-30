package backend

import (
	"context"
	"encoding/json"
	"github.com/theruknology/govessel/internal/inference"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestOpenAIRequestAndUsage(t *testing.T) {
	b := OpenAI{URL: "http://localhost:8000/v1/chat/completions", Model: "test", APIKey: "test-secret"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b.Client = &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer test-secret" {
			t.Error("bad upstream request")
		}
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("deadline not propagated")
		}
		var payload struct {
			Model     string              `json:"model"`
			MaxTokens int                 `json:"max_tokens"`
			Messages  []map[string]string `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload.Model != "test" || payload.MaxTokens != 32 || payload.Messages[0]["content"] != "hello" {
			t.Error(payload)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"hello back"}}],"usage":{"completion_tokens":7}}`)), Header: make(http.Header)}, nil
	})}
	result, err := b.Infer(ctx, inference.Request{Tenant: "a", Prompt: "hello", MaxOutputTokens: 32})
	if err != nil || result.Text != "hello back" || result.OutputTokens != 7 {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestOpenAIRejectsBadResponses(t *testing.T) {
	for _, body := range []string{`{}`, `{"choices":[{"message":{"content":"ok"}}]}`, `{"choices":[{}],"usage":{"completion_tokens":-1}}`, `{"choices":[{}],"usage":{"completion_tokens":999}}`, `{"choices":[{}],"usage":{"completion_tokens":7}}`, `not json`, strings.Repeat("x", (8<<20)+1)} {
		b := OpenAI{URL: "http://localhost/v1/chat/completions", Model: "test", Client: &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})}}
		if _, err := b.Infer(context.Background(), inference.Request{MaxOutputTokens: 16}); err == nil {
			t.Fatal("invalid upstream response accepted")
		}
	}
}
func TestSyntheticCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Synthetic{TokenTime: time.Second}).Infer(ctx, inference.Request{Tenant: "a", MaxOutputTokens: 10}); err != context.Canceled {
		t.Fatal(err)
	}
}
