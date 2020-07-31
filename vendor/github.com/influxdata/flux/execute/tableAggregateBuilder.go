package execute

import (
	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

type aggregateTableBuilderCache struct {
	inner tableBuilderCache
}

func (a *aggregateTableBuilderCache) Table(key flux.GroupKey) (flux.Table, error) {
	return a.inner.Table(key)
}

func (a *aggregateTableBuilderCache) ForEach(f func(flux.GroupKey)) {
	panic("implement me")
}

func (a *aggregateTableBuilderCache) ForEachWithContext(f func(flux.GroupKey, Trigger, TableContext)) {
	panic("implement me")
}

func (a *aggregateTableBuilderCache) DiscardTable(key flux.GroupKey) {
	panic("implement me")
}

func (a *aggregateTableBuilderCache) ExpireTable(key flux.GroupKey) {
	panic("implement me")
}

func (a *aggregateTableBuilderCache) SetTriggerSpec(t plan.TriggerSpec) {
	panic("implement me")
}

func (a *aggregateTableBuilderCache) TableBuilder(key flux.GroupKey) (TableBuilder, bool) {
	d := a.inner
	b, ok := d.lookupState(key)
	if !ok {
		builder := NewColListTableBuilder(key, d.alloc)
		t := NewTriggerFromSpec(d.triggerSpec)
		b = tableState{
			builder: builder,
			trigger: t,
		}
		d.tables.Set(key, b)
	}
	return b.builder, !ok
}

func (a *aggregateTableBuilderCache) ForEachBuilder(f func(flux.GroupKey, TableBuilder)) {
	panic("implement me")
}

type aggregateTableBuilder struct {
	table   TableBuilder
	aggFunc []ValueFunc
	vals    []array.Builder
	agg     Aggregate
	config  AggregateConfig
	nCol    int
	colMap  map[int]int
	cols    []flux.ColMeta
}

func NewAggregateTableBuilder(b TableBuilder, agg Aggregate, config AggregateConfig) TableBuilder {
	return &aggregateTableBuilder{
		table:  b,
		agg:    agg,
		config: config,
		colMap: map[int]int{},
	}
}
func (a *aggregateTableBuilder) Key() flux.GroupKey {
	return a.table.Key()
}

func (a *aggregateTableBuilder) NRows() int {
	return a.table.NRows()
}

func (a *aggregateTableBuilder) NCols() int {
	return a.table.NRows()
}

func (a *aggregateTableBuilder) Cols() []flux.ColMeta {
	return a.cols
}

func (t *aggregateTableBuilder) AddCol(c flux.ColMeta) (int, error) {
	t.cols = append(t.cols, c)
	found := false
	for _, label := range t.config.Columns {
		if c.Label == label {
			found = true
			t.colMap[t.nCol] = len(t.colMap)
		}
	}
	t.nCol++
	if !found {
		return -1, nil
	}
	var vf ValueFunc
	switch c.Type {
	case flux.TBool:
		vf = t.agg.NewBoolAgg()
		t.vals = append(t.vals, array.NewBooleanBuilder(memory.DefaultAllocator))
	case flux.TInt:
		vf = t.agg.NewIntAgg()
		t.vals = append(t.vals, array.NewInt64Builder(memory.DefaultAllocator))
	case flux.TUInt:
		vf = t.agg.NewUIntAgg()
		t.vals = append(t.vals, array.NewUint64Builder(memory.DefaultAllocator))
	case flux.TFloat:
		vf = t.agg.NewFloatAgg()
		t.vals = append(t.vals, array.NewFloat64Builder(memory.DefaultAllocator))
	case flux.TString:
		vf = t.agg.NewStringAgg()
	}
	if vf == nil {
		return 0, errors.Newf(codes.FailedPrecondition, "unsupported aggregate column type %v", c.Type)
	}
	t.aggFunc = append(t.aggFunc, vf)
	return t.table.AddCol(c)

}

func (a *aggregateTableBuilder) SetValue(i, j int, value values.Value) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) SetNil(i, j int) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) AppendBool(j int, value bool) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) AppendInt(j int, value int64) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) AppendUInt(j int, value uint64) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) AppendFloat(j int, value float64) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) AppendString(j int, value string) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) AppendTime(j int, value Time) error {
	return nil
}

func (t *aggregateTableBuilder) AppendValue(j int, v values.Value) error {
	i, ok := t.colMap[j]
	if !ok {
		return nil
	}
	vs := t.vals[i]
	switch v.Type() {
	case semantic.Bool:
		vs.(*array.BooleanBuilder).Append(v.Bool())
	case semantic.Int:
		vs.(*array.Int64Builder).Append(v.Int())
	case semantic.UInt:
		vs.(*array.Uint64Builder).Append(v.UInt())
	case semantic.Float:
		vs.(*array.Float64Builder).Append(v.Float())
	default:
		return errors.Newf(codes.Invalid, "unsupported aggregate type %v", v.Type())
	}
	if vs.Len() == 16 {
		t.compute()
	}
	return nil
}
func (t *aggregateTableBuilder) compute() error {
	for j, vf := range t.aggFunc {
		vs := t.vals[j]
		switch vf.Type() {
		case flux.TBool:
			vf.(DoBoolAgg).DoBool(vs.NewArray().(*array.Boolean))
		case flux.TInt:
			vf.(DoIntAgg).DoInt(vs.NewArray().(*array.Int64))
		case flux.TUInt:
			vf.(DoUIntAgg).DoUInt(vs.NewArray().(*array.Uint64))
		case flux.TFloat:
			vf.(DoFloatAgg).DoFloat(vs.NewArray().(*array.Float64))
		case flux.TString:
			vf.(DoStringAgg).DoString(vs.NewArray().(*array.Binary))
		default:
			return errors.Newf(codes.Invalid, "unsupported aggregate type %v", vf.Type())
		}
	}
	return nil

}
func (a *aggregateTableBuilder) AppendNil(j int) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) AppendBools(j int, vs *array.Boolean) error {
	f := a.aggFunc[j]
	f.(DoBoolAgg).DoBool(vs)
	return nil
}

func (a *aggregateTableBuilder) AppendInts(j int, vs *array.Int64) error {
	a.aggFunc[j].(DoIntAgg).DoInt(vs)
	return nil
}

func (a *aggregateTableBuilder) AppendUInts(j int, vs *array.Uint64) error {
	a.aggFunc[j].(DoUIntAgg).DoUInt(vs)
	return nil
}

func (a *aggregateTableBuilder) AppendFloats(j int, vs *array.Float64) error {
	a.aggFunc[j].(DoFloatAgg).DoFloat(vs)
	return nil
}

func (a *aggregateTableBuilder) AppendStrings(j int, vs *array.Binary) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) AppendTimes(j int, vs *array.Int64) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) GrowBools(j, n int) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) GrowInts(j, n int) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) GrowUInts(j, n int) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) GrowFloats(j, n int) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) GrowStrings(j, n int) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) GrowTimes(j, n int) error {
	panic("implement me")
}

func (a *aggregateTableBuilder) LevelColumns() error {
	panic("implement me")
}

func (a *aggregateTableBuilder) Sort(cols []string, desc bool) {
	panic("implement me")
}

func (a *aggregateTableBuilder) ClearData() {
	a.colMap = map[int]int{}
	a.nCol = 0
	a.table.ClearData()
	a.config.Columns = nil
	a.vals = nil
	a.aggFunc = nil
	a.agg = nil
}

func (a *aggregateTableBuilder) Release() {
	a.table.Release()
}

func (a *aggregateTableBuilder) Table() (flux.Table, error) {
	a.compute()
	builder := a.table
	for i, c := range builder.Cols() {
		vf := a.aggFunc[i]
		bj := i
		err := func() error {
			switch c.Type {
			case flux.TBool:
				v := vf.(BoolValueFunc).ValueBool()
				if err := builder.AppendBool(bj, v); err != nil {
					return err
				}
			case flux.TInt:
				v := vf.(IntValueFunc).ValueInt()
				if err := builder.AppendInt(bj, v); err != nil {
					return err
				}
			case flux.TUInt:
				v := vf.(UIntValueFunc).ValueUInt()
				if err := builder.AppendUInt(bj, v); err != nil {
					return err
				}
			case flux.TFloat:
				v := vf.(FloatValueFunc).ValueFloat()
				if err := builder.AppendFloat(bj, v); err != nil {
					return err
				}
			case flux.TString:
				v := vf.(StringValueFunc).ValueString()
				if err := builder.AppendString(bj, v); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	tidx, err := builder.AddCol(flux.ColMeta{Type: flux.TTime,Label: DefaultTimeColLabel})
	if err != nil {
		return nil, err
	}
	for j, c := range builder.Key().Cols() {
		if c.Label == DefaultStartColLabel {
			t := builder.Key().Value(j)
			err := builder.SetValue(0,tidx, t)
			if err != nil {
				return nil, err
			}
			break
		}
	}
	return builder.Table()
}
func NewAggregateTableBuilderCache(agg Aggregate, config AggregateConfig, a *memory.Allocator) *tableBuilderCache {
	return &tableBuilderCache{
		tables: NewGroupLookup(),
		alloc:  a,
		builderFn: func(key flux.GroupKey, a *memory.Allocator) TableBuilder {
			b := NewColListTableBuilder(key, a)
			return NewAggregateTableBuilder(b, agg, config)
		},
	}
}
