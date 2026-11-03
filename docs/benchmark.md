# Local measurements

Measured on 2026-11-03 inside WSL 2, Linux amd64, AMD Ryzen 7 7735HS, 12 logical processors visible to Go, Go 1.27.3. All runs use synthetic service times or an immediate backend. Host load and WSL scheduling affect wall-clock measurements.

## Deterministic simulation

Reproduce:

```sh
make sim
```

Eight scenarios × 20 seeds (1–20), each with a 30-second virtual arrival window. Every scenario/seed is run against both policies, so the output has 320 JSONL records. The full matrix took about **one real second** locally, excluding Go compilation. Tests check capacity, terminal accounting, determinism, backend failure/cancellation injection, and non-starvation with fixed assertions across the same 160 combinations and 10-second windows.

For each row below, timely/offered pools all 20 seeds. Latency is the **mean of the 20 per-seed completed-request p99s**, not a pooled p99. All offered counts include early rejections. Successful responses, rejected requests, stale work, expirations, cancellations, and backend failures are separate counters in the raw JSON.

| Scenario | Offered | Baseline timely/offered | Vessel timely/offered | Baseline mean completed p99 | Vessel mean completed p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Overload | 90,143 | 3.5% | 33.6% | 1,499.6 ms | 647.0 ms |
| Burst | 47,501 | 54.1% | 51.6% | 1,485.2 ms | 649.1 ms |
| Noisy neighbor | 21,117 | 23.7% | 42.7% | 1,498.4 ms | 1,249.5 ms |
| Output shift | 21,019 | 53.5% | 67.6% | 1,449.5 ms | 826.8 ms |
| Backend slowdown | 21,019 | 67.9% | 71.5% | 1,402.1 ms | 1,219.8 ms |
| Backend errors | 20,658 | 69.6% | 69.3% | 202.4 ms | 194.4 ms |
| Cancellations | 20,899 | 70.4% | 70.4% | 152.6 ms | 153.0 ms |
| Tight deadlines | 21,203 | 45.9% | 34.8% | 128.1 ms | 112.6 ms |

The overload scenario offers 150 requests/s against four slots with mean service around 80 ms, approximately 3× nominal capacity. A 33.6% offered-request success rate is therefore close to the useful-work capacity limit. Rejected and shed requests count against timely/offered.

The baseline honors deadlines and cancels running work when time runs out. Under sustained FIFO backlog, it often starts work with too little time left to complete. Both policies have four execution slots.

Vessel completes fewer offered requests in the burst and tight-deadline scenarios, despite lower tail latency. Admission depends on the initial predictor and safety factor; some rejected requests would have finished in time.

A longer run:

```sh
./bin/sim -scenario overload -seed 1 -seeds 1 -duration 10000s
```

This replayed **10,000 virtual seconds for both policies in 5.03 real seconds** locally, including workload generation and report calculation, excluding compilation. Ordered arrivals are merged directly with the completion heap to keep heap size proportional to outstanding work.

The simulator models independent execution slots with synthetic service times, not continuous GPU batching, KV-cache pressure, prefill/decode asymmetry, retries, or a distributed replica fleet.

## Gateway overhead at 2,000 offered requests/s

Reproduce:

```sh
make load
# Or: go run ./cmd/bench -rps 2000 -duration 5s
```

After 100 warmup requests, a scheduled-arrival driver offers 10,000 requests over five seconds to the in-process gateway with Prometheus metrics enabled and an immediate backend. Each request has a one-second deadline. The run had no failures:

| Measurement | Result |
| --- | ---: |
| Gateway call p50, less measured backend body time | 18.9 μs |
| Gateway call p99, less measured backend body time | 118.5 μs |
| Scheduled-arrival-to-completion p99 | 1.281 ms |
| Driver elapsed time | 5.000 s |

Gateway-call overhead includes dispatch, synchronization, queue wait, lifecycle management, and metrics. It excludes HTTP/gRPC transport, model execution, and the driver's delay before it can launch the scheduled request. The scheduled-arrival metric includes that driver delay so driver scheduling delays appear in the results. All failures are counted separately, and the command exits unsuccessfully if any fail; latency quantiles cover successful requests only.

These runs do not measure end-to-end network latency.

## Microbenchmarks

```sh
make bench
```

| Benchmark | Time/op | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Policy enqueue + dispatch + completion | 425 ns | 152 | 3 |
| Gateway + immediate backend, metrics disabled | 3,732 ns | 528 | 9 |

These are average benchmark times, not latency percentiles. They use one active tenant. They do not characterize lock contention, thousands of distinct tenants, network serialization, or real-model inference throughput.
