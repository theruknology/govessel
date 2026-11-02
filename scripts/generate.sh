#!/usr/bin/env sh
set -eu
# Refresh the checked-in protobuf bindings after schema changes.
command -v protoc >/dev/null 2>&1 || { echo 'Install protoc v33.0 first.' >&2; exit 1; }
tools_dir="$(mktemp -d)"
trap 'rm -rf "$tools_dir"' EXIT
GOBIN="$tools_dir" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
GOBIN="$tools_dir" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
PATH="$tools_dir:$PATH" protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative api/vessel/v1/inference.proto
