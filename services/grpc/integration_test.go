package grpc

import (
	"context"
	"encoding/json"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	values2 "github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"log"
	"net"
	"testing"
)

type FluxControllerMock struct {
	QueryFn func(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
}

func NewFluxControllerMock() *FluxControllerMock {
	return &FluxControllerMock{
		QueryFn: func(ctx context.Context, compiler flux.Compiler) (query flux.Query, e error) {
			p, err := compiler.Compile(ctx)
			if err != nil {
				return nil, err
			}
			alloc := &memory.Allocator{}
			return p.Start(ctx, alloc)
		},
	}
}

func (m *FluxControllerMock) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	return m.QueryFn(ctx, compiler)
}

func (m *FluxControllerMock) PrometheusCollectors() []prometheus.Collector { return nil }
func Test_all(t *testing.T) {
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

	request := ProxyRequest{
		Spec: flux.Spec{},
	}
	body, err := json.Marshal(request)
	if err != nil {
		panic(err)
	}
	client, err := c.ExecSpec(context.TODO(), &datatypes.SpecRequest{Request: body})
	if err != nil {
		panic(err)
	}
	resp, err := client.Recv()
	if err != nil {
		panic(err)
	}
	groupKey := func() flux.GroupKey {
		var cols []flux.ColMeta
		var values []values2.Value
		for _, keys := range resp.GroupKeys.Values {
			log.Println(resp.GroupKeys)
			t, err := values2.NewFromString(semantic.Nature(keys.Nature), keys.Str)
			if err != nil {
				panic(err)
			}
			values = append(values, t)

		}
		for _, c := range resp.GroupKeys.Meta {
			cols = append(cols, flux.ColMeta{
				Label: c.Label,
				Type:  flux.ColType(c.Type),
			})
		}
		return execute.NewGroupKey(cols, values)
	}()
	var values [] array.Interface
	colMeta := func() []flux.ColMeta {
		var cols []flux.ColMeta
		for i, m := range resp.ColumnMeta {
			cols = append(cols, flux.ColMeta{
				Label: m.Label,
				Type:  flux.ColType(m.Type),
			})
			builder := arrow.NewBuilder(flux.ColType(m.Type), &memory.Allocator{})

			switch flux.ColType(m.Type) {
			case flux.TTime:
				for _, v := range resp.Frames[i].GetIntegerPoints().Values {
					arrow.AppendInt(builder, v)

				}
			case flux.TFloat:
				for _, v := range resp.Frames[i].GetFloatPoints().Values {
					arrow.AppendFloat(builder, v)

				}
			case flux.TInt:
				for _, v := range resp.Frames[i].GetIntegerPoints().Values {
					arrow.AppendInt(builder, v)

				}

			}
			values = append(values, builder.NewArray())

		}
		return cols
	}()
	tb := &arrow.TableBuffer{
		GroupKey: groupKey,
		Columns:  colMeta,
		Values:   values,
	}
	cache := execute.NewTableBuilderCache(&memory.Allocator{})
	cols := tb.Cols()
	on := make(map[string]bool, len(cols))
	for _, k := range tb.Key().Cols() {
		on[k.Label] = true
	}

	colMap := make([]int, 0, len(tb.Cols()))
	cr := tb
	l := cr.Len()
	log.Println(l)
	for i := 0; i < l; i++ {
		log.Println(i)
		key := execute.GroupKeyForRowOn(i, cr, on)
		cache.SetTriggerSpec(plan.AfterWatermarkTriggerSpec{})
		builder, _ := cache.TableBuilder(key)

		colMap, err := AddNewTableCols(cols, builder, colMap)
		if err != nil {
			panic(err)
		}

		err = execute.AppendMappedRecordWithNulls(i, cr, builder, colMap)
		if err != nil {
			panic(err)
		}
	}
	log.Println(cache)
	cache.ForEachBuilder(func(key flux.GroupKey, builder execute.TableBuilder) {
		t, _ := builder.Table()
		testT, _ := executetest.ConvertTable(t)
		log.Println(testT)

	})

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


