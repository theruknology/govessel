package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/theruknology/govessel/internal/inference"
)

// Synthetic models a single inference slot. It generates no real model output.
type Synthetic struct{ TokenTime time.Duration }

func (s Synthetic) Infer(ctx context.Context, r inference.Request) (inference.Result, error) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(r.Tenant + ":" + r.Prompt))
	output := 1 + int(h.Sum32()%uint32(r.MaxOutputTokens))
	timer := time.NewTimer(time.Duration(inference.PromptTokens(r.Prompt)+output) * s.TokenTime)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return inference.Result{}, ctx.Err()
	case <-timer.C:
	}
	return inference.Result{Text: fmt.Sprintf("Synthetic response (%d tokens); no model was called.", output), OutputTokens: output}, nil
}

type OpenAI struct {
	URL, Model, APIKey string
	Client             *http.Client
}

func (b OpenAI) Validate() error {
	u, err := url.Parse(b.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return fmt.Errorf("upstream must be an absolute http(s) URL without credentials")
	}
	if b.Model == "" {
		return fmt.Errorf("upstream model is required")
	}
	return nil
}
func (b OpenAI) Infer(ctx context.Context, r inference.Request) (inference.Result, error) {
	payload := struct {
		Model     string              `json:"model"`
		Messages  []map[string]string `json:"messages"`
		MaxTokens int                 `json:"max_tokens"`
	}{b.Model, []map[string]string{{"role": "user", "content": r.Prompt}}, r.MaxOutputTokens}
	data, err := json.Marshal(payload)
	if err != nil {
		return inference.Result{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.URL, bytes.NewReader(data))
	if err != nil {
		return inference.Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if b.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.APIKey)
	}
	client := b.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return inference.Result{}, fmt.Errorf("upstream transport: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return inference.Result{}, fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
	}
	const maxBody = 8 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return inference.Result{}, err
	}
	if len(body) > maxBody {
		return inference.Result{}, fmt.Errorf("upstream response too large")
	}
	var decoded struct {
		Choices []struct {
			Message struct {
				Content *string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			CompletionTokens *int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err = json.Unmarshal(body, &decoded); err != nil {
		return inference.Result{}, fmt.Errorf("invalid upstream JSON: %w", err)
	}
	if len(decoded.Choices) == 0 || decoded.Choices[0].Message.Content == nil || decoded.Usage.CompletionTokens == nil || *decoded.Usage.CompletionTokens < 0 || *decoded.Usage.CompletionTokens > r.MaxOutputTokens {
		return inference.Result{}, fmt.Errorf("upstream response lacks valid choices or completion token usage")
	}
	return inference.Result{Text: strings.TrimSpace(*decoded.Choices[0].Message.Content), OutputTokens: *decoded.Usage.CompletionTokens}, nil
}
