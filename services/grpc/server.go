package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/types"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/flux/semantic"
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

func (s *server) ExecSpec(r *datatypes.SpecRequest, stream datatypes.Storage_ExecSpecServer) error {
	defer log.Println("ennd#####################")
	req := new(ProxyRequest)
	err := json.Unmarshal(r.Request, req)
	if err != nil {
		log.Println(err)
	}
	q, err := s.controller.Query(context.TODO(), repl.Compiler{
		Spec: &req.Spec,
	})
	req.Spec.Walk(func(o *flux.Operation) error {
		log.Println(o.Spec.Kind(), ":", o.ID)
		return nil
	})
	if err != nil {
		log.Println(err)
		return err
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
				var groupKeyMeta []*datatypes.TableResponse_ColMeta
				var values []*datatypes.TableResponse_Value
				for i, c := range groupKey.Cols() {
					/*label := c.Label
					if label == execute.DefaultStartColLabel || label == execute.DefaultStopColLabel {
						continue
					}*/
					groupKeyMeta = append(groupKeyMeta, &datatypes.TableResponse_ColMeta{
						Label: c.Label,
						Type:  int32(c.Type),
					})

					v := groupKey.Value(i)
					nature := v.Type().(semantic.Nature)
					tNature := int32(nature)
					switch c.Type {
					case flux.TTime:
						values = append(values, &datatypes.TableResponse_Value{
							Str:    v.Time().String(),
							Nature: int32(nature),
						})
					case flux.TString:
						values = append(values, &datatypes.TableResponse_Value{
							Str:    v.Str(),
							Nature: tNature,
						})
					case flux.TFloat:
						values = append(values, &datatypes.TableResponse_Value{
							Str:    fmt.Sprintf("%f", v.Float()),
							Nature: tNature,
						})
					case flux.TInt:
						values = append(values, &datatypes.TableResponse_Value{
							Str:    fmt.Sprintf("%v", v.Int()),
							Nature: tNature,
						})
					case flux.TUInt:
						values = append(values, &datatypes.TableResponse_Value{
							Str:    fmt.Sprintf("%v", v.UInt()),
							Nature: tNature,
						})
					case flux.TBool:
						values = append(values, &datatypes.TableResponse_Value{
							Str:    fmt.Sprintf("%v", v.Bool()),
							Nature: tNature,
						})

					}

				}
				response := datatypes.TableResponse{
					GroupKeys: &datatypes.TableResponse_GroupKeys{
						Meta:   groupKeyMeta,
						Values: values,
					},
				}
				hasTime := false
				for _, c := range reader.Cols() {
					if c.Label == "_time" {
						hasTime = true
					}
				}
				for i, c := range reader.Cols() {
					label := c.Label
					if !hasTime {
						if label == "_start" {
							label = "_time"
						}
					}
					if label == execute.DefaultStartColLabel || label == execute.DefaultStopColLabel {
						continue
					}
					if label != execute.DefaultValueColLabel && label != "_time" {
						continue
					}
					response.ColumnMeta = append(response.ColumnMeta, &datatypes.TableResponse_ColMeta{
						Label: label,
						Type:  int32(c.Type),
					})
					var frame datatypes.TableResponse_Frame
					switch c.Type {
					case flux.TTime:
						/*for _,iv:=range reader.Times(i).Int64Values(){
							log.Println(time.Unix(0,iv))
						}*/
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

					case flux.TUInt:
						frame.Data = &datatypes.TableResponse_Frame_UnsignedPoints{
							UnsignedPoints: &datatypes.TableResponse_UnsignedPointsFrame{
								Values: reader.UInts(i).Uint64Values(),
							},
						}
					case flux.TString:
						var strs []string
						s := reader.Strings(i)
						l := s.Len()
						for j := 0; j < l; j++ {
							strs = append(strs, s.ValueString(j))
						}
						frame.Data = &datatypes.TableResponse_Frame_StringPoints{
							StringPoints: &datatypes.TableResponse_StringPointsFrame{
								Values: strs,
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

func (s *server) Serve(ln net.Listener) *grpc.Server {
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
