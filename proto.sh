#!/usr/bin/env bash
protoc --go_out . cluster/internal/data.proto
protoc -I storage/reads/datatypes/:vendor/github.com/gogo/protobuf/ --go_out=plugins=grpc:storage/reads/datatypes predicate.proto storage_common.proto
