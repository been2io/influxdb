package grpc

import (
	"context"
	"encoding/json"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/flux/semantic"
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

func (s *server) ExecSpec(r *datatypes.SpecRequest, stream datatypes.Storage_ExecSpecServer) error {
	req := new(ProxyRequest)
	err := json.Unmarshal(r.Request, req)
	if err != nil {
		log.Println(err)
	}
	q, err := s.controller.Query(context.TODO(), repl.Compiler{
		Spec: &req.Spec,
	})
	if err != nil {
		panic(err)
	}
	resultIterator := flux.NewResultIteratorFromQuery(q)
	for resultIterator.More() {
		if err := resultIterator.Err(); err != nil {
			return err
		}
		result := resultIterator.Next()
		return result.Tables().Do(func(table flux.Table) error {
			return table.Do(func(reader flux.ColReader) error {
				groupKey := reader.Key()
				log.Println("key", groupKey)
				var groupKeyMeta []*datatypes.TableResponse_ColMeta
				var values []*datatypes.TableResponse_Value
				for i, c := range groupKey.Cols() {
					groupKeyMeta = append(groupKeyMeta, &datatypes.TableResponse_ColMeta{
						Label: c.Label,
						Type:  int32(c.Type),
					})

					v := groupKey.Value(i)
					log.Println(v)
					nature := v.Type().(semantic.Nature)
					values = append(values, &datatypes.TableResponse_Value{
						Str:    v.Time().String(),
						Nature: int32(nature),
					})

				}
				response := datatypes.TableResponse{
					GroupKeys: &datatypes.TableResponse_GroupKeys{
						Meta:   groupKeyMeta,
						Values: values,
					},
				}
				for i, c := range reader.Cols() {
					response.ColumnMeta = append(response.ColumnMeta, &datatypes.TableResponse_ColMeta{
						Label: c.Label,
						Type:  int32(c.Type),
					})
					var frame datatypes.TableResponse_Frame
					switch c.Type {
					case flux.TTime:
						frame.Data = &datatypes.TableResponse_Frame_IntegerPoints{
							IntegerPoints: &datatypes.TableResponse_IntegerPointsFrame{
								Values: reader.Times(i).Int64Values(),
							},
						}
					case flux.TBool:
						frame.Data = &datatypes.TableResponse_Frame_BooleanPoints{
							BooleanPoints: &datatypes.TableResponse_BooleanPointsFrame{
								Values: reader.Bools(i).NullBitmapBytes(),
							},
						}
					case flux.TInt:
						frame.Data = &datatypes.TableResponse_Frame_IntegerPoints{
							IntegerPoints: &datatypes.TableResponse_IntegerPointsFrame{
								Values: reader.Ints(i).Int64Values(),
							},
						}
					case flux.TFloat:
						frame.Data = &datatypes.TableResponse_Frame_FloatPoints{
							FloatPoints: &datatypes.TableResponse_FloatPointsFrame{
								Values: reader.Floats(i).Float64Values(),
							},
						}

					}

					response.Frames = append(response.Frames, frame)
				}

				return stream.Send(&response)
			})
		})

	}
	return nil
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
	return err
}

func (*server) TagKeys(*datatypes.TagKeysRequest, datatypes.Storage_TagKeysServer) error {

	panic("implement me")
}

func (*server) TagValues(*datatypes.TagValuesRequest, datatypes.Storage_TagValuesServer) error {
	panic("implement me")
}

func (s *server) Serve(ln net.Listener) {
	grpcServer := grpc.NewServer()
	datatypes.RegisterStorageServer(grpcServer, s)
	go func() {
		if err := grpcServer.Serve(ln); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
}
