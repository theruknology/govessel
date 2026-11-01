package sim

import (
	"reflect"
	"testing"
	"time"
)

// Fixed invariants are checked across every scenario and seed.
func TestFaultMatrix(t *testing.T) {
	const seeds = 20
	for _, scenario := range Scenarios {
		t.Run(scenario, func(t *testing.T) {
			for seed := int64(1); seed <= seeds; seed++ {
				work, err := Workload(scenario, seed, 10*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				for _, baseline := range []bool{false, true} {
					first, err := Run(scenario, seed, work, baseline)
					if err != nil {
						t.Fatalf("seed=%d baseline=%t: %v", seed, baseline, err)
					}
					second, err := Run(scenario, seed, work, baseline)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(first, second) {
						t.Fatalf("nondeterministic replay: seed %d", seed)
					}
					if first.Offered == 0 || first.Timely > first.Admitted || first.Admitted > first.Offered || first.TimelyAdmitted > 1 || first.PeakRunning > 4 || first.PeakQueue > 1024 || first.Admitted != first.Completed+first.Shed+first.Expired+first.Canceled+first.Failed {
						t.Fatalf("accounting/capacity failure: %+v", first)
					}
					if baseline && first.Shed != 0 {
						t.Fatal("baseline unexpectedly used CoDel")
					}
					switch scenario {
					case "backend_errors":
						if first.Failed == 0 {
							t.Fatalf("seed %d: no backend failures exercised", seed)
						}
					case "cancellations":
						if first.Canceled == 0 {
							t.Fatalf("seed %d: no cancellations exercised", seed)
						}
					case "deadline_crunch":
						if first.Rejected+first.Expired == 0 {
							t.Fatal("no short deadlines exercised")
						}
					case "noisy_neighbor":
						if first.TokensByTenant["tenant-0"] == 0 || first.TokensByTenant["heavy"] == 0 {
							t.Fatal("tenant starved")
						}
					}
				}
			}
		})
	}
}
func TestIdenticalWorkloadAndUnknownScenario(t *testing.T) {
	a, _ := Workload("overload", 42, time.Second)
	b, _ := Workload("overload", 42, time.Second)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("workload is nondeterministic")
	}
	if _, err := Workload("imaginary", 1, time.Second); err == nil {
		t.Fatal("unknown scenario accepted")
	}
}
func TestImpossibleDeadlinesRejectedBeforeExecution(t *testing.T) {
	work := []Work{{Tenant: "a", Prompt: 16, Output: 64, MaxOutput: 128, Budget: time.Millisecond, Service: 80 * time.Millisecond}}
	report, err := Run("unit", 1, work, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Rejected != 1 || report.WastedServiceSeconds != 0 || report.Admitted != 0 {
		t.Fatal(report)
	}
	baseline, err := Run("unit", 1, work, true)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Expired != 1 || baseline.WastedServiceSeconds <= 0 {
		t.Fatal(baseline)
	}
}
