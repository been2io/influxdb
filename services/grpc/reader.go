package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"log"
	"sync"
)

type NodeReader struct {
	Addr []string
	RP   string
	DB   string
}

func (r *NodeReader) Read(ctx context.Context, spec flux.Spec, f func(r flux.ColReader) error) error {
	b, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	var clone flux.Spec
	err = json.Unmarshal(b, &clone)
	if err != nil {
		return err
	}
	client := Client{
		Addrs: r.Addr,
	}
	clone.Walk(func(o *flux.Operation) error {
		log.Println(o.Spec.Kind())
		if o.Spec.Kind() == influxdb.FromKind {
			s := o.Spec
			from, ok := s.(*influxdb.FromOpSpec)
			if  ok {
				from.Bucket = fmt.Sprintf("%v/%v", r.DB, r.RP)
			}
		}
		return nil
	})
	return client.Read(ctx, clone, f)
}

type Reader struct {
	NodeReaders []*NodeReader
}
type StageSourceHandler interface {
	UpdateWatermark(time int64) error
	Finish(err error)
}

func (r *Reader) read(ctx context.Context, handler StageSourceHandler, spec flux.Spec, tables *tables) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	for _, client := range r.NodeReaders {
		wg.Add(1)
		go func(c *NodeReader) {
			err := c.Read(ctx, spec, tables.Add)
			if err != nil {
				cancel()
				handler.Finish(err)
			}
			wg.Done()
		}(client)
	}
	go func() {
		wg.Wait()
		tables.Done()
	}()
	return nil
}
func (r *Reader) Read(ctx context.Context, spec flux.Spec) (flux.TableIterator, error) {
	tables := NewTables(100)
	handler, ok := ctx.Value("source").(StageSourceHandler)
	if !ok {
		return nil, errors.New("reader require source handler")
	}
	go r.read(ctx, handler, spec, tables)
	return tables, nil
}
