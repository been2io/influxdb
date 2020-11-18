#!/bin/bash
# Run the build utility via Docker
go build \
    -a \
    -o output/bin/influxd \
    cmd/influxd/main.go

