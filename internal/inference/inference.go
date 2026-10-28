package inference

import "context"

type Request struct {
	Tenant          string `json:"tenant_id"`
	Prompt          string `json:"prompt"`
	MaxOutputTokens int    `json:"max_output_tokens"`
}
type Result struct {
	RequestID    uint64 `json:"request_id"`
	Text         string `json:"text"`
	OutputTokens int    `json:"output_tokens"`
}
type Backend interface {
	Infer(context.Context, Request) (Result, error)
}

// PromptTokens is a conservative portable approximation, not a model tokenizer.
// Production integrations should supply a tokenizer matching the upstream model.
func PromptTokens(prompt string) int { return (len(prompt) + 3) / 4 }
