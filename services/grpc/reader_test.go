package grpc

import (
	"context"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	"log"
	"net"
	"testing"
	"time"
)

type testsourcehanlder struct {
}

func (t testsourcehanlder) UpdateWatermark(time int64) error {
	return nil
}

func (t testsourcehanlder) Finish(err error) {
	log.Println(err)
}

func TestReader(t *testing.T) {
	lis, err := net.Listen("tcp", "")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
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
				{Label: "_ttime", Type: flux.TTime},
				{Label: "_value", Type: flux.TFloat},
				{Label: "_field", Type: flux.TString},
			},
			Data: [][]interface{}{
				{execute.Time(0), execute.Time(100), execute.Time(0), 0.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(10), 1.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(20), 2.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(30), 3.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(40), 4.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(50), 5.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(60), 6.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(70), 7.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(80), 8.0, "a"},
				{execute.Time(0), execute.Time(100), execute.Time(90), 9.0, "a"},
			},
		}})
		return &q, nil
	}
	srv := Service{
		Listener:   lis,
		Store:      &fakeStore{},
		Controller: controller,
	}
	err = srv.Open()
	if err != nil {
		panic(err)
	}
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
	ctx := context.WithValue(context.Background(), "source", testsourcehanlder{})
	ti, err := reader.Read(ctx, expQ)
	if err != nil {
		panic(err)
	}
	ti.Do(func(table flux.Table) error {

		log.Println(table.Empty())
		table.Do(func(reader flux.ColReader) error {
			log.Println(reader.Key().String(), reader.Len())
			for i := 0; i < len(reader.Cols()); i++ {
				c := reader.Cols()[i]
				log.Println(i, ":", c)
				for i := 0; i < reader.Len(); i++ {
				}
			}

			return nil
		})
		return nil
	})

}

func TestReadNoData(t *testing.T) {
	lis, err := net.Listen("tcp", "")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	controller := NewFluxControllerMock()
	controller.QueryFn = func(ctx context.Context, compiler flux.Compiler) (query flux.Query, e error) {
		q := Query{
			ResultsCh: make(chan flux.Result, 1),
		}
		close(q.ResultsCh)
		return &q, nil
	}
	srv := Service{
		Listener:   lis,
		Store:      &fakeStore{},
		Controller: controller,
	}
	err = srv.Open()
	if err != nil {
		panic(err)
	}
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
	ctx := context.WithValue(context.Background(), "source", testsourcehanlder{})
	ti, err := reader.Read(ctx, expQ)
	if err != nil {
		panic(err)
	}
	err = ti.Do(func(table flux.Table) error {

		log.Println(table.Empty())
		table.Do(func(reader flux.ColReader) error {
			log.Println(reader.Key().String())
			for i := 0; i < reader.Len(); i++ {

			}
			return nil
		})
		return nil
	})
	if err != nil {
		panic(err)
	}

}
