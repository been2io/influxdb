package grpc

import (
	"context"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"google.golang.org/grpc"
	"log"
	"net"
	"testing"
	"time"
)

func TestReader(t *testing.T) {
	lis, err := net.Listen("tcp", ":8899")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	controller := NewFluxControllerMock()
	controller.QueryFn = func(ctx context.Context, compiler flux.Compiler) (query flux.Query, e error) {
		q := Query{
			ResultsCh: make(chan flux.Result, 1),
		}
		q.ResultsCh <- executetest.NewResult([]*executetest.Table{{
			KeyCols: []string{"_start", "_stop"},
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TFloat},
			},
			Data: [][]interface{}{
				{execute.Time(0), execute.Time(100), execute.Time(0), 0.0},
				{execute.Time(0), execute.Time(100), execute.Time(10), 1.0},
				{execute.Time(0), execute.Time(100), execute.Time(20), 2.0},
				{execute.Time(0), execute.Time(100), execute.Time(30), 3.0},
				{execute.Time(0), execute.Time(100), execute.Time(40), 4.0},
				{execute.Time(0), execute.Time(100), execute.Time(50), 5.0},
				{execute.Time(0), execute.Time(100), execute.Time(60), 6.0},
				{execute.Time(0), execute.Time(100), execute.Time(70), 7.0},
				{execute.Time(0), execute.Time(100), execute.Time(80), 8.0},
				{execute.Time(0), execute.Time(100), execute.Time(90), 9.0},
			},
		}})
		return &q, nil
	}
	srv := server{
		store:      &fakeStore{},
		controller: controller,
	}
	datatypes.RegisterStorageServer(s, &srv)
	srv.Serve(lis)
	time.Sleep(time.Second)
	reader := Reader{
		Addr: [][]string{[]string{lis.Addr().String()}},
	}
	expQ := flux.Spec{
		Operations: []*flux.Operation{
			{
				ID: "from",
				Spec: &influxdb.FromOpSpec{
					Bucket: "mybucket",
				},
			},
			{
				ID: "range",
				Spec: &universe.RangeOpSpec{
					Start: flux.Time{
						Relative:   -4 * time.Hour,
						IsRelative: true,
					},
					Stop: flux.Time{
						IsRelative: true,
					},
				},
			},
			{
				ID:   "sum",
				Spec: &universe.SumOpSpec{},
			},
		},
		Edges: []flux.Edge{
			{Parent: "from", Child: "range"},
			{Parent: "range", Child: "sum"},
		},
	}
	_, err = reader.Read(expQ)
	if err!=nil{
		panic(err)
	}
}
