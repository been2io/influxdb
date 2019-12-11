package main

import (
	"context"
	"encoding/json"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {
	lis, err := net.Listen("tcp", ":8899")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	srv := server{
		store: &fakeStore{},
	}
	datatypes.RegisterStorageServer(s, &srv)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	address := lis.Addr().String()
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := datatypes.NewStorageClient(conn)
	r := reads.NewReader(&influxdb.RemoteStore{Client: c})
	ti, err := r.ReadFilter(context.Background(), influxdb.ReadFilterSpec{
		Bounds: execute.Bounds{
			Start: execute.Time(0),
			Stop:  execute.Time(30),
		},
	}, &memory.Allocator{})
	if err != nil {
		log.Fatalf("unexpected error: %s", err)
	}

	var got []*executetest.Table
	if err := ti.Do(func(tbl flux.Table) error {
		t, err := executetest.ConvertTable(tbl)
		log.Println("table", *t)
		if err != nil {
			return err
		}

		got = append(got, t)
		return nil
	}); err != nil {
		log.Fatalf("unexpected error: %s", err)
	}
	// Contact the server and print out its response.

}

type fakeStore struct {
}

func (fs *fakeStore) ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
	inputs := make([]cursors.Cursor, 2)
	closed := 0
	inputs[0] = func() cursors.Cursor {
		called := false
		cur := mock.NewFloatArrayCursor()
		cur.NextFunc = func() *cursors.FloatArray {
			if called {
				return &cursors.FloatArray{}
			}
			called = true
			return &cursors.FloatArray{
				Timestamps: []int64{0},
				Values:     []float64{1.0},
			}
		}
		cur.CloseFunc = func() {
			closed++
		}
		return cur
	}()
	inputs[1] = func() cursors.Cursor {
		called := false
		cur := mock.NewIntegerArrayCursor()
		cur.NextFunc = func() *cursors.IntegerArray {
			if called {
				return &cursors.IntegerArray{}
			}
			called = true
			return &cursors.IntegerArray{
				Timestamps: []int64{10},
				Values:     []int64{1},
			}
		}
		cur.CloseFunc = func() {
			closed++
		}
		return cur
	}()

	idx := -1
	rs := mock.NewResultSet()
	rs.NextFunc = func() bool {
		idx++
		return idx < len(inputs)
	}
	rs.CursorFunc = func() cursors.Cursor {
		return inputs[idx]
	}
	rs.CloseFunc = func() {
		idx = len(inputs)
	}
	return rs, nil
}
func (fs *fakeStore) ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error) {
	panic("no implement")
}

type server struct {
	controller interface {
		Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
		PrometheusCollectors() []prometheus.Collector
	}
	store interface {
		ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error)
		ReadGroup(ctx context.Context, req *datatypes.ReadGroupRequest) (reads.GroupResultSet, error)
	}
}

func (s *server) ExecSpec(r *datatypes.SpecRequest, stream datatypes.Storage_ExecSpecServer) error {
	request := new(ProxyRequest)
	err := json.Unmarshal(r.Request, request)
	if err != nil {
		return err
	}
	q, err := s.controller.Query(context.TODO(), request.Request.Compiler)
	resultIterator := flux.NewResultIteratorFromQuery(q)
	w := reads.NewResponseWriter(stream, datatypes.HintFlags(datatypes.HintNoPoints))
	for resultIterator.More() {
		if err := resultIterator.Err(); err != nil {
			return err
		}
		result := resultIterator.Next()
		result.Tables().Do(func(table flux.Table) error {
			return table.Do(func(reader flux.ColReader) error {
				reader.Bools(0).Value(1)
				return nil
			})
		})

	}
	w.WriteResultSet(resultIterator)
	if err != nil {
		return err
	}
	panic("implement me")
}

func (s *server) Capabilities(context.Context, *empty.Empty) (*datatypes.CapabilitiesResponse, error) {
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
