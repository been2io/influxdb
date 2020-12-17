package grpc

import (
	"context"
	"errors"
	"github.com/influxdata/influxdb/services/replication"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"io"
	"log"
	"time"
)

type grpcClient struct {
	client datatypes.StorageClient
}

func (g *grpcClient) RequestPartitionShards(ctx context.Context, req replication.ShardRequest) ([]replication.ShardInfo, error) {
	shards, err := g.client.RequestPartitionShards(ctx, &datatypes.ShardRequest{
		DB:        req.DB,
		Partition: req.Partition,
		Start:     req.Start.Unix(),
		End:       req.End.Unix(),
	})
	if err != nil {
		return nil, err
	}
	var shardInfo []replication.ShardInfo
	for _, shard := range shards.Items {
		shardInfo = append(shardInfo, replication.ShardInfo{
			ID:    shard.ID,
			Start: time.Unix(shard.Start, 0),
			End:   time.Unix(shard.End, 0),
		})
	}
	return shardInfo, nil
}

type rw struct {
	buf chan []byte
}

func (r *rw) Read(p []byte) (n int, err error) {
	p1, ok := <-r.buf
	if !ok {
		return 0, io.EOF
	}
	p = p1
	return len(p1), nil
}

func (r *rw) Write(p []byte) (n int, err error) {
	r.buf <- p
	return len(p), nil
}

func (r *rw) Close() error {
	close(r.buf)
	return nil
}

func (g *grpcClient) ExportShard(ctx context.Context, req replication.ShardExportRequest) (io.Reader, error) {
	r, err := g.client.ExportShard(ctx, &datatypes.ShardExportRequest{
		ID:    req.ID,
		Start: req.Start.Unix(),
		End:   req.End.Unix(),
	})
	if err != nil {
		return nil, err
	}
	buf := rw{buf: make(chan []byte,100)}
	go func() {
		for {
			resp, err := r.Recv()
			log.Println("message")
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err == nil {
				buf.Write(resp.Data)
			} else if err == io.EOF {
				buf.Close()
			} else {
				panic(err)
			}

		}
	}()

	return &buf, nil
}
func (g *grpcClient) ImportPartition(ctx context.Context, req replication.PartitionImportRequest) error {
	r, err := g.client.ImportPartition(ctx, &datatypes.PartitionImportRequest{
		Addr:      req.Addr,
		DB:        req.DB,
		Partition: req.Partition,
		Start:     req.Start.Unix(),
		End:       req.End.Unix(),
	})
	if err != nil {
		return err
	}
	if r.Err != "" {
		return errors.New(r.Err)
	}
	return nil
}
