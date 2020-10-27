package http

import (
	"context"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"google.golang.org/grpc"
	"net"
)

type store interface {
	ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error)
	ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error)
	ReadWindowAggregate(ctx context.Context, spec query.ReadWindowAggregateSpec, alloc *memory.Allocator) (query.TableIterator, error)
}
type grpcServer struct {
	store store
}

func (s *grpcServer) Capabilities(context.Context, *types.Empty) (*datatypes.CapabilitiesResponse, error) {
	panic("implement me")
}
func (s *grpcServer) ReadWindowAggregate(r *datatypes.ReadWindowAggregateRequest, stream datatypes.Storage_ReadWindowAggregateServer) error {
	rs, err := s.store.ReadFilter(context.Background(), &datatypes.ReadFilterRequest{
		ReadSource: r.ReadSource,
		Range:      r.Range,
	})
	if err != nil {
		return err
	}
	w := reads.NewResponseWriter(stream, datatypes.HintFlags(datatypes.HintNone))
	rs, err = reads.NewWindowAggregateResultSet(context.Background(), r, rs.Cursor().(reads.SeriesCursor))
	if err != nil {
		return err
	}
	err = w.WriteResultSet(rs)
	w.Flush()
	return err
}
func (s *grpcServer) ReadFilter(r *datatypes.ReadFilterRequest, stream datatypes.Storage_ReadFilterServer) error {
	rs, err := s.store.ReadFilter(context.TODO(), r)
	if err != nil {
		return err
	}
	w := reads.NewResponseWriter(stream, datatypes.HintFlags(datatypes.HintNone))
	err = w.WriteResultSet(rs)
	w.Flush()
	return err

}

func (s *grpcServer) ReadGroup(r *datatypes.ReadGroupRequest, stream datatypes.Storage_ReadGroupServer) error {
	rs, err := s.store.ReadGroup(context.TODO(), r)
	if err != nil {
		return err
	}
	w := reads.NewResponseWriter(stream, datatypes.HintFlags(datatypes.HintNone))
	err = w.WriteGroupResultSet(rs)
	w.Flush()
	return err
}

func (s *grpcServer) TagKeys(*datatypes.TagKeysRequest, datatypes.Storage_TagKeysServer) error {

	panic("implement me")
}

func (s *grpcServer) TagValues(*datatypes.TagValuesRequest, datatypes.Storage_TagValuesServer) error {
	panic("implement me")
}

func (s *grpcServer) Serve(ln net.Listener) *grpc.Server {
	grpcServer := grpc.NewServer()
	datatypes.RegisterStorageServer(grpcServer, s)
	return grpcServer
	/*go func() {
		log.Printf("start grpc on addr %v", ln.Addr())
		if err := grpcServer.Serve(ln); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()*/
}
