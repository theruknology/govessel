.PHONY: run build test race vet check fmt sim bench load proto docker

run:
	go run ./cmd/vessel

build:
	mkdir -p bin
	go build -trimpath -o bin/vessel ./cmd/vessel
	go build -trimpath -o bin/sim ./cmd/sim
	go build -trimpath -o bin/bench ./cmd/bench

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: vet race
	@test -z "$$(gofmt -l cmd internal api)" || (gofmt -l cmd internal api; exit 1)

fmt:
	gofmt -w cmd internal api

sim:
	mkdir -p results
	go run ./cmd/sim -scenario all -seed 1 -seeds 20 -duration 30s > results/simulation.jsonl

bench:
	go test ./internal/policy ./internal/gateway -run '^$$' -bench . -benchmem

load:
	mkdir -p results
	go run ./cmd/bench -rps 2000 -duration 5s > results/gateway-load.json

proto:
	./scripts/generate.sh

docker:
	docker compose up --build
