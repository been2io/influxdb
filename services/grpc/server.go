package grpc

import (
	"context"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/flux"
	_ "github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"log"
	"net"
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
	controller controller
	store      store
}

func (s *server) Capabilities(context.Context, *types.Empty) (*datatypes.CapabilitiesResponse, error) {
	panic("implement me")
}

func (s *server) ReadFilter(r *datatypes.ReadFilterRequest, stream datatypes.Storage_ReadFilterServer) error {
	rs, err := s.store.ReadFilter(context.TODO(), r)
	if err != nil {
		return err
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
	w := reads.NewResponseWriter(stream, datatypes.HintFlags(datatypes.HintNone))
	err = w.WriteGroupResultSet(rs)
	w.Flush()
	return err
}

func (*server) TagKeys(*datatypes.TagKeysRequest, datatypes.Storage_TagKeysServer) error {

	panic("implement me")
}

func (*server) TagValues(*datatypes.TagValuesRequest, datatypes.Storage_TagValuesServer) error {
	panic("implement me")
}

func (s *server) Serve(ln net.Listener)  {
	grpcServer := grpc.NewServer()
	datatypes.RegisterStorageServer(grpcServer, s)
	log.Println("start grpc", ln.Addr().String())
	if err := grpcServer.Serve(ln); err != nil {
		log.Fatalf("grpc failed to serve: %v", err)
	}
}
