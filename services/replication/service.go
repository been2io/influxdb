package replication

import (
	"context"
	"encoding"
	"fmt"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type TSDBStore interface {
	BackupShard(id uint64, since time.Time, w io.Writer) error
	ExportShard(id uint64, ExportStart time.Time, ExportEnd time.Time, w io.Writer) error
	Shard(id uint64) *tsdb.Shard
	ShardRelativePath(id uint64) (string, error)
	SetShardEnabled(shardID uint64, enabled bool) error
	RestoreShard(id uint64, r io.Reader) error
	CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
	ImportShard(id uint64, r io.Reader) error
}
type MetaClient interface {
	encoding.BinaryMarshaler
	Database(name string) *meta.DatabaseInfo
	CreateShardGroup(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
	ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error)
}
type Service struct {
	wg         sync.WaitGroup
	MetaClient MetaClient
	TSDBStore  TSDBStore
	NewClient  func(addr string) (Client, error)
	Logger     *zap.Logger
}

func (s *Service) RequestPartitionShards(ctx context.Context, req ShardRequest) []ShardInfo {
	dbMeta := s.MetaClient.Database(req.DB)
	rpInfo := dbMeta.RetentionPolicy(req.Partition)
	var groups []ShardInfo
	for _, sg := range rpInfo.ShardGroups {
		if sg.Overlaps(req.Start, req.End) {
			for _, sh := range sg.Shards {
				groups = append(groups, ShardInfo{
					ID:    sh.ID,
					Start: sg.StartTime,
					End:   sg.EndTime,
				})
			}
		}
	}
	return groups
}
func (s *Service) ExportShard(ctx context.Context, req ShardExportRequest, w io.Writer) error {
	return s.TSDBStore.ExportShard(req.ID, req.Start, req.End, w)
}
func (s *Service) ImportPartition(ctx context.Context, req PartitionImportRequest) error {
	peer, err := s.NewClient(req.Addr)
	if err != nil {
		return err
	}
	shards, err := peer.RequestPartitionShards(ctx, ShardRequest{
		DB:        req.DB,
		Partition: req.Partition,
		Start:     req.Start,
		End:       req.End,
	})
	if err != nil {
		return err
	}
	for _, shard := range shards {
		r, err := peer.ExportShard(ctx, ShardExportRequest{
			ID:    shard.ID,
			Start: shard.Start,
			End:   shard.End,
		})
		err = s.TSDBStore.ImportShard(shard.ID, r)
		if err != nil {
			return err
		}
	}
	return err

}
func (s *Service) ImportShard(ctx context.Context, req ShardImportRequest) error {
	peer, err := s.NewClient(req.Addr)
	if err != nil {
		return err
	}
	r, err := peer.ExportShard(ctx, ShardExportRequest{
		ID:    req.ID,
		Start: req.Start,
		End:   req.End,
	})
	if err != nil {
		return err
	}
	shards, err := s.MetaClient.ShardGroupsByTimeRange(req.DB, req.Partition, req.Start, req.End)
	if err != nil {
		return nil
	}
	if len(shards) < 1 {
		sg, err := s.MetaClient.CreateShardGroup(req.DB, req.Partition, req.Start)
		if err != nil {
			return err
		}
		shards = append(shards, *sg)
	}
	var ids []uint64
	for _, shard := range shards {
		for _, s := range shard.Shards {
			ids = append(ids, s.ID)
		}

	}
	if len(ids) > 1 {
		return fmt.Errorf("unexpected shard err: %v %v", req.DB, req.Partition)
	}
	err = s.TSDBStore.ImportShard(ids[0], r)
	return err
}
