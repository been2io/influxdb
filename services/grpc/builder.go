package grpc

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"log"
	"sync"
)

type mergeTableBuilder struct {
	cache execute.TableBuilderCache
}

func newTableBuilder() *mergeTableBuilder {
	b := &mergeTableBuilder{}
	cache := execute.NewTableBuilderCache(&memory.Allocator{})
	cache.SetTriggerSpec(plan.AfterWatermarkTriggerSpec{})
	b.cache = cache
	return b
}
func (t *mergeTableBuilder) add(tb flux.ColReader) error {
	cols := tb.Cols()
	on := make(map[string]bool, len(cols))
	for _, k := range tb.Key().Cols() {
		on[k.Label] = true
	}
	colMap := make([]int, 0, len(tb.Cols()))
	cr := tb
	l := cr.Len()
	for i := 0; i < l; i++ {
		key := execute.GroupKeyForRowOn(i, cr, on)
		builder, _ := t.cache.TableBuilder(key)
		colMap, err := AddNewTableCols(cols, builder, colMap)
		if err != nil {
			return err
		}
		err = execute.AppendMappedRecordWithNulls(i, cr, builder, colMap)
		if err != nil {
			return err
		}
	}
	return nil
}

type mergeTable struct {
	readers chan flux.ColReader
	key     flux.GroupKey
	cols    []flux.ColMeta
}

func newMergeTable(key flux.GroupKey, cols []flux.ColMeta) *mergeTable {
	return &mergeTable{
		key:     key,
		cols:    cols,
		readers: make(chan flux.ColReader, 100),
	}
}
func (t *mergeTable) Key() flux.GroupKey {
	return t.key
}
func (t *mergeTable) Add(cr flux.ColReader) {
	t.readers <- cr
}
func (t *mergeTable) Cols() []flux.ColMeta {
	return t.cols
}

func (t *mergeTable) Do(f func(flux.ColReader) error) error {
	for r := range t.readers {
		if err := f(r); err != nil {
			return err
		}
	}
	return nil
}

func (t *mergeTable) Done() {
	close(t.readers)
}

func (t *mergeTable) Empty() bool {
	if len(t.readers) != 0 {
		return false
	}
	return true
}
func NewTables(size int) *tables {
	return &tables{m: map[string]*mergeTable{}, new: make(chan *mergeTable, size)}
}

type tables struct {
	m   map[string]*mergeTable
	new chan *mergeTable
	err chan error
	rw  sync.RWMutex
}

func (cr *tables) Do(f func(flux.Table) error) error {
	for v := range cr.new {
		if err := f(v); err != nil {
			return err
		}
	}
	return nil
}

func (cr *tables) Add(reader flux.ColReader) error {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}

	}()
	key := reader.Key().String()
	var new bool
	cr.rw.Lock()
	r, ok := cr.m[key]
	if !ok {
		r = newMergeTable(reader.Key(), reader.Cols())
		cr.m[key] = r
		new = true
	}
	cr.rw.Unlock()
	if new {
		cr.new <- r
	}
	r.Add(reader)
	return nil
}
func (cr *tables) Done() {
	close(cr.new)
	for _, v := range cr.m {
		v.Done()
	}

}
