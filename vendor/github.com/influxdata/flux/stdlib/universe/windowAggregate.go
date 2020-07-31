package universe

import (
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/plan"
)

func init()  {
	//plan.RegisterLogicalRules(&WindowAggregateMergeRule{})
	execute.RegisterTransformation(windowAggregate,createWindowAggregateTransformation)
}
const windowAggregate = "windowAggregate"

type windowAggregateProcedureSpec struct {
	*WindowProcedureSpec
	AggregateConfig execute.AggregateConfig
	aggMethod       plan.ProcedureKind
}

func createWindowAggregateProcedureSpec(spec plan.ProcedureSpec, config execute.AggregateConfig, method plan.ProcedureKind) (plan.ProcedureSpec, error) {
	s, ok := spec.(*WindowProcedureSpec)
	if !ok {
		return nil, errors.Newf(codes.Internal, "window aggregate not window spec: %v", spec)
	}
	return &windowAggregateProcedureSpec{
		WindowProcedureSpec: s,
		AggregateConfig:     config,
		aggMethod:           method,
	}, nil
}
func (w *windowAggregateProcedureSpec) Kind() plan.ProcedureKind {
	return windowAggregate
}

func (w *windowAggregateProcedureSpec) Copy() plan.ProcedureSpec {
	return w
}

func createWindowAggregateTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*windowAggregateProcedureSpec)
	if !ok {
		return nil, nil, errors.Newf(codes.Internal, "invalid spec type %T", spec)
	}
	var agg execute.Aggregate
	switch s.aggMethod {
	case SumKind:
		agg = new(SumAgg)
	default:
		return nil, nil, errors.Newf(codes.Internal, "invalid method %v spec type %T", s.aggMethod, spec)
	}
	cache := execute.NewAggregateTableBuilderCache(agg, s.AggregateConfig, a.Allocator())
	d := execute.NewDataset(id, mode, cache)

	bounds := a.StreamContext().Bounds()
	if bounds == nil {
		return nil, nil, errors.New(codes.Invalid, "nil bounds passed to window")
	}

	w, err := execute.NewWindow(
		s.Window.Every,
		s.Window.Period,
		s.Window.Offset,
	)
	if err != nil {
		return nil, nil, err
	}
	t := NewFixedWindowTransformation(
		d,
		cache,
		*bounds,
		w,
		s.TimeColumn,
		s.StartColumn,
		s.StopColumn,
		s.CreateEmpty,
	)
	return t, d, nil
}

type WindowAggregateMergeRule struct {
}

func (w *WindowAggregateMergeRule) Name() string {
	return "windowAggregate"
}

func (w *WindowAggregateMergeRule) Pattern() plan.Pattern {
	return plan.Pat(SumKind, plan.Pat(WindowKind, plan.Any()))
}

func (w *WindowAggregateMergeRule) Rewrite(node plan.Node) (plan.Node, bool, error) {
	switch node.Kind() {
	case SumKind:
		s := node.ProcedureSpec().(*SumProcedureSpec)
		w := node.Predecessors()[0]
		spec, err := createWindowAggregateProcedureSpec(w.ProcedureSpec(), s.AggregateConfig, s.Kind())
		if err != nil {
			return nil, false, err
		}
		n, err := plan.MergeToLogicalNode(node, w, spec)
		if err != nil {
			return nil, false, err
		}
		return n, true, nil
	}
	panic("implement me")
}
