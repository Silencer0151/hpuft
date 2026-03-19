#!/bin/bash
set -e
go mod download
go build -o hpuft ./cmd/hpuft
