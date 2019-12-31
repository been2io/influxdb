package grpc

import "github.com/influxdata/flux"

type table struct {
	key     flux.GroupKey
	colMeta []flux.ColMeta
	i       int
	buffers []flux.ColReader
}

func (tb *table) Key() flux.GroupKey {
	return tb.key
}

func (tb *table) Cols() []flux.ColMeta {
	return tb.colMeta
}

func (tb *table) Do(f func(flux.ColReader) error) error {
	defer tb.Done()
	for ; tb.i < len(tb.buffers); tb.i++ {
		b := tb.buffers[tb.i]
		if err := f(b); err != nil {
			return err
		}
		b.Release()
	}
	return nil
}

func (tb *table) Done() {
	for ; tb.i < len(tb.buffers); tb.i++ {
		tb.buffers[tb.i].Release()
	}
}

func (tb *table) Empty() bool {
	return len(tb.buffers) == 0
}

func (tb *table) Copy() flux.BufferedTable {
	for i := tb.i; i < len(tb.buffers); i++ {
		tb.buffers[i].Retain()
	}
	return &table{
		key:     tb.key,
		colMeta: tb.colMeta,
		i:       tb.i,
		buffers: tb.buffers,
	}
}






