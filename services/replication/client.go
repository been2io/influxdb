package replication

import (
	"context"
	"io"
)

type Client interface {
	RequestPartitionShards(ctx context.Context, req ShardRequest) ([]ShardInfo, error)
	ExportShard(ctx context.Context, req ShardExportRequest) (io.Reader, error)
}

