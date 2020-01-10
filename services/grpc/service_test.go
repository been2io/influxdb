package grpc

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/flux/stdlib/universe"
	"log"
	"testing"
	"time"
)

func TestService(t *testing.T) {
	reader := Reader{
		Addr: [][]string{[]string{"localhost:8899"}},
	}
	expQ := flux.Spec{
		Operations: []*flux.Operation{
			{
				ID: "from",
				Spec: &influxdb.FromOpSpec{
					Bucket: "a",
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
	ti, err := reader.Read(expQ)
	if err != nil {
		panic(err)
	}

	err=ti.Do(func(table flux.Table) error {

		log.Println(table.Empty())
		table.Do(func(reader flux.ColReader) error {
			log.Println(reader.Key().String())
			return nil
		})
		return nil
	})
	if err!=nil{
		panic(err)
	}

}
