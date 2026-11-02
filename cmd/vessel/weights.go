package main

import (
	"encoding/json"
	"fmt"
	"github.com/theruknology/govessel/internal/policy"
)

func parseWeights(raw string, cfg *policy.Config) error {
	if err := json.Unmarshal([]byte(raw), &cfg.Weights); err != nil {
		return fmt.Errorf("weights: %w", err)
	}
	return nil
}
