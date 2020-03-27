package grpc

import (
	"context"
	"errors"
	"github.com/influxdata/flux"
	"sync"
)

type Reader struct {
	Addr [][]string
}
type StageSourceHandler interface {
	UpdateWatermark(time int64) error
	Finish(err error)
}

func (r *Reader) read(ctx context.Context, handler StageSourceHandler, spec flux.Spec, tables *tables) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	for _, addr := range r.Addr {
		wg.Add(1)
		go func(addr []string) {
			client := Client{
				Addrs: addr,
			}
			err := client.Read(ctx, spec,tables.Add)
			if err != nil {
				cancel()
				handler.Finish(err)
				return
			}
			wg.Done()
		}(addr)
	}
	wg.Wait()
	tables.Done()
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
