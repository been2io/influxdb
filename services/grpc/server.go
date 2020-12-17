package grpc

import (
	"context"
	"github.com/influxdata/flux"
	_ "github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/services/replication"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net"
	"time"
)

type controller interface {
	Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
	PrometheusCollectors() []prometheus.Collector
}
type store interface {
	ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error)
	ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error)
}
type server struct {
	controller         controller
	store              store
	replicationService replication.Service
}

func NewServer(controller controller, store store, metaClient replication.MetaClient, tsdbStore replication.TSDBStore) *server {
	service := replication.Service{
		MetaClient: metaClient,
		TSDBStore:  tsdbStore,
	}
	service.NewClient = func(addr string) (replication.Client, error) {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		client := datatypes.NewStorageClient(conn)
		return &grpcClient{
			client: client,
		}, nil
	}
	return &server{
		controller:         controller,
		store:              store,
		replicationService: service,
	}
}
func (s *server) Capabilities(ctx context.Context, empty *emptypb.Empty) (*datatypes.CapabilitiesResponse, error) {
	panic("implement me")
}

func (s *server) Write(ctx context.Context, request *datatypes.WriteShardRequest) (*datatypes.WriteResponse, error) {
	panic("implement me")
}

func (s *server) RequestPartitionShards(ctx context.Context, request *datatypes.ShardRequest) (*datatypes.ShardInfoList, error) {
	infoList := s.replicationService.RequestPartitionShards(ctx, replication.ShardRequest{
		DB:        request.DB,
		Partition: request.Partition,
		Start:     time.Unix(request.Start, 0),
		End:       time.Unix(request.End, 0),
	})
	var items []*datatypes.ShardInfo
	for _, info := range infoList {
		items = append(items, &datatypes.ShardInfo{
			ID:    info.ID,
			Start: info.Start.Unix(),
			End:   info.End.Unix(),
		})
	}
	return &datatypes.ShardInfoList{Items: items}, nil
}

type ShardStream struct {
	BytesCount int
	writer     datatypes.Storage_ExportShardServer
}

func (s *ShardStream) Close() error {
	return nil
}

func (s *ShardStream) Write(p []byte) (n int, err error) {
	err = s.writer.Send(&datatypes.ShardExportResponse{
		Data: p,
	})
	size := len(p)
	log.Println("############ export shard bytes:",size)
	s.BytesCount = s.BytesCount + size
	return size, err
}

func (s *server) ExportShard(request *datatypes.ShardExportRequest, shardServer datatypes.Storage_ExportShardServer) error {
	w := ShardStream{
		writer: shardServer,
	}
	r := replication.ShardExportRequest{
		ID:    request.ID,
		Start: time.Unix(request.Start, 0),
		End:   time.Unix(request.End, 0),
	}
	err := s.replicationService.ExportShard(context.Background(), r, &w)
	log.Println("export shard size:",w.BytesCount)
	return err
}

func (s *server) ImportPartition(ctx context.Context, request *datatypes.PartitionImportRequest) (*datatypes.PartitionImportResponse, error) {

	err := s.replicationService.ImportPartition(ctx, replication.PartitionImportRequest{
		Start:     time.Unix(request.Start, 0),
		End:       time.Unix(request.End, 0),
		Addr:      request.Addr,
		DB:        request.DB,
		Partition: request.Partition,
	})
	if err != nil {
		return nil, err
	}
	return &datatypes.PartitionImportResponse{}, nil
}

func (s *server) ReadFilter(r *datatypes.ReadFilterRequest, stream datatypes.Storage_ReadFilterServer) error {
	rs, err := s.store.ReadFilter(context.TODO(), r)
	if err != nil {
		return err
	}
	if rs == nil {
		return nil
	}
	w := reads.NewResponseWriter(stream, datatypes.HintFlags(datatypes.HintNone))
	err = w.WriteResultSet(rs)
	w.Flush()
	return err

}

func (s *server) ReadGroup(r *datatypes.ReadGroupRequest, stream datatypes.Storage_ReadGroupServer) error {
	rs, err := s.store.ReadGroup(context.TODO(), r)
	if err != nil {
		return err
	}
	if rs == nil {
		return nil
	}
	w := reads.NewResponseWriter(stream, datatypes.HintFlags(datatypes.HintNone))
	err = w.WriteGroupResultSet(rs)
	w.Flush()
	return err
}

func (s *server) TagKeys(*datatypes.TagKeysRequest, datatypes.Storage_TagKeysServer) error {

	panic("implement me")
}

func (s *server) TagValues(*datatypes.TagValuesRequest, datatypes.Storage_TagValuesServer) error {
	panic("implement me")
}
func (s *server) Server() *grpc.Server {
	grpcServer := grpc.NewServer()
	datatypes.RegisterStorageServer(grpcServer, s)
	return grpcServer
}
func (s *server) Serve(ln net.Listener) {
	grpcServer := grpc.NewServer()
	datatypes.RegisterStorageServer(grpcServer, s)
	log.Println("start grpc", ln.Addr().String())
	if err := grpcServer.Serve(ln); err != nil {
		log.Fatalf("grpc failed to serve: %v", err)
	}
}
