package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/theruknology/govessel/internal/sim"
	"os"
	"time"
)

func main() {
	scenario := flag.String("scenario", "all", "scenario name or all")
	seed := flag.Int64("seed", 1, "first seed for deterministic replay")
	seeds := flag.Int("seeds", 20, "number of seeds")
	duration := flag.Duration("duration", 30*time.Second, "simulated arrival window")
	flag.Parse()
	if *seeds < 1 || *seeds > 10000 || *duration <= 0 || *duration > 24*time.Hour {
		fmt.Fprintln(os.Stderr, "invalid seeds or duration")
		os.Exit(2)
	}
	names := sim.Scenarios
	if *scenario != "all" {
		names = []string{*scenario}
	}
	encoder := json.NewEncoder(os.Stdout)
	start := time.Now()
	for _, name := range names {
		for n := 0; n < *seeds; n++ {
			current := *seed + int64(n)
			work, err := sim.Workload(name, current, *duration)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			for _, baseline := range []bool{true, false} {
				report, err := sim.Run(name, current, work, baseline)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				if err := encoder.Encode(report); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
			}
		}
	}
	fmt.Fprintf(os.Stderr, "replayed %d scenario-seed combinations, both policies, in %s\n", len(names)*(*seeds), time.Since(start))
}
