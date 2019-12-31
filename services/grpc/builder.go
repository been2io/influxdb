package grpc

import (
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
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
	return f(<-t.readers)
}

func (mergeTable) Done() {
	panic("implement me")
}

func (mergeTable) Empty() bool {
	panic("implement me")
}

type tables struct {
	m      map[string]*mergeTable
}

func (cr *tables) Do(f func(flux.Table) error) error {
	for _, v := range cr.m {
		if err := f(v); err != nil {
			return err
		}
	}
	return nil
}

func (cr *tables) Add(reader flux.ColReader) {
	key := reader.Key().String()
	r, ok := cr.m[key]
	if !ok {
		r = newMergeTable(reader.Key(), reader.Cols())
		cr.m[key] = r
	}
	r.Add(reader)
}
