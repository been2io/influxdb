package universe

import (
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/internal/errors"
	"github.com/influxdata/flux/internal/execute/table"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
)

const DerivativeWindowKind = "derivativeWindow"

type DerivativeWindowOpSpec struct {
	Unit        flux.Duration `json:"unit"`
	NonNegative bool          `json:"nonNegative"`
	Columns     []string      `json:"columns"`
	TimeColumn  string        `json:"timeColumn"`
	Every       flux.Duration `json:"every"`
	Period      flux.Duration `json:"period"`
	Offset      flux.Duration `json:"offset"`
}

func init() {
	derivativeWindowSignature := flux.FunctionSignature(
		map[string]semantic.PolyType{
			"unit":        semantic.Duration,
			"nonNegative": semantic.Bool,
			"columns":     semantic.NewArrayPolyType(semantic.String),
			"timeColumn":  semantic.String,
			"every":       semantic.Duration,
			"period":      semantic.Duration,
			"offset":      semantic.Duration,
		},
		nil,
	)

	flux.RegisterPackageValue("universe", DerivativeWindowKind, flux.FunctionValue(DerivativeWindowKind, createDerivativeWindowOpSpec, derivativeWindowSignature))
	flux.RegisterOpSpec(DerivativeWindowKind, newDerivativeWindowOp)
	plan.RegisterProcedureSpec(DerivativeWindowKind, newDerivativeWindowProcedure, DerivativeWindowKind)
	execute.RegisterTransformation(DerivativeWindowKind, createDerivativeWindowTransformation)
}

func createDerivativeWindowOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(DerivativeWindowOpSpec)

	if unit, ok, err := args.GetDuration("unit"); err != nil {
		return nil, err
	} else if ok {
		spec.Unit = unit
	} else {
		// Default is 1s
		spec.Unit = flux.ConvertDuration(time.Second)
	}

	if nn, ok, err := args.GetBool("nonNegative"); err != nil {
		return nil, err
	} else if ok {
		spec.NonNegative = nn
	}
	if timeCol, ok, err := args.GetString("timeColumn"); err != nil {
		return nil, err
	} else if ok {
		spec.TimeColumn = timeCol
	} else {
		spec.TimeColumn = execute.DefaultTimeColLabel
	}

	if cols, ok, err := args.GetArray("columns", semantic.String); err != nil {
		return nil, err
	} else if ok {
		columns, err := interpreter.ToStringArray(cols)
		if err != nil {
			return nil, err
		}
		spec.Columns = columns
	} else {
		spec.Columns = []string{execute.DefaultValueColLabel}
	}
	derivativeDefaultEvery := flux.ConvertDuration(time.Minute)
	if every, ok, err := args.GetDuration("every"); err != nil {
		return nil, err
	} else if ok {
		spec.Every = every
	} else {
		spec.Every = derivativeDefaultEvery
	}
	if period, ok, err := args.GetDuration("period"); err != nil {
		return nil, err
	} else if ok {
		spec.Period = period
	} else {
		spec.Period = derivativeDefaultEvery
	}
	if offset, ok, err := args.GetDuration("offset"); err != nil {
		return nil, err
	} else if ok {
		spec.Offset = offset
	} else {
		spec.Offset = derivativeDefaultEvery
	}
	return spec, nil
}

func newDerivativeWindowOp() flux.OperationSpec {
	return new(DerivativeWindowOpSpec)
}

func (s *DerivativeWindowOpSpec) Kind() flux.OperationKind {
	return DerivativeWindowKind
}

type DerivativeWindowProcedureSpec struct {
	plan.DefaultCost
	Unit        flux.Duration   `json:"unit"`
	NonNegative bool            `json:"non_negative"`
	Columns     []string        `json:"columns"`
	TimeColumn  string          `json:"timeColumn"`
	Window      plan.WindowSpec `json:"timeColumn"`
}

func newDerivativeWindowProcedure(qs flux.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*DerivativeWindowOpSpec)
	if !ok {
		return nil, errors.Newf(codes.Internal, "invalid spec type %T", qs)
	}

	return &DerivativeWindowProcedureSpec{
		Unit:        spec.Unit,
		NonNegative: spec.NonNegative,
		Columns:     spec.Columns,
		TimeColumn:  spec.TimeColumn,
		Window: plan.WindowSpec{
			Every:  spec.Every,
			Period: spec.Period,
			Offset: spec.Offset,
		},
	}, nil
}

func (s *DerivativeWindowProcedureSpec) Kind() plan.ProcedureKind {
	return DerivativeWindowKind
}
func (s *DerivativeWindowProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(DerivativeWindowProcedureSpec)
	*ns = *s
	if s.Columns != nil {
		ns.Columns = make([]string, len(s.Columns))
		copy(ns.Columns, s.Columns)
	}
	return ns
}

// TriggerSpec implements plan.TriggerAwareProcedureSpec
func (s *DerivativeWindowProcedureSpec) TriggerSpec() plan.TriggerSpec {
	return plan.NarrowTransformationTriggerSpec{}
}

func createDerivativeWindowTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*DerivativeWindowProcedureSpec)
	if !ok {
		return nil, nil, errors.Newf(codes.Internal, "invalid spec type %T", spec)
	}
	cache := execute.NewTableBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewDerivativeWindowTransformation(d, cache, s)
	window, err := execute.NewWindow(s.Window.Every, s.Window.Period, s.Window.Offset)
	if err != nil {
		return nil, nil, err
	}
	t.window = window

	return t, d, nil
}

type Last struct {
	last       int
	lastTs     int64
	lastBounds execute.Bounds
	window     execute.Window
}

func (l *Last) shifted(tt int64) bool {
	ttt := execute.Time(tt)
	if l.lastBounds.IsEmpty() {
		l.lastBounds = l.window.GetEarliestBounds(ttt)
	} else if !l.lastBounds.Contains(ttt) {
		for ; !l.lastBounds.Contains(ttt); l.lastBounds = l.lastBounds.Shift(l.window.Every) {
		}
		return true
	}
	l.lastTs = tt
	return false
}

type derivativeWindowWindowTransformation struct {
	d     execute.Dataset
	cache execute.TableBuilderCache

	unit        float64
	nonNegative bool
	columns     []string
	timeCol     string
	window      execute.Window
	last        Last
}

func NewDerivativeWindowTransformation(d execute.Dataset, cache execute.TableBuilderCache, spec *DerivativeWindowProcedureSpec) *derivativeWindowWindowTransformation {
	return &derivativeWindowWindowTransformation{
		d:           d,
		cache:       cache,
		unit:        float64(values.Duration(spec.Unit).Duration()),
		nonNegative: spec.NonNegative,
		columns:     spec.Columns,
		timeCol:     spec.TimeColumn,
	}
}

func (t *derivativeWindowWindowTransformation) RetractTable(id execute.DatasetID, key flux.GroupKey) error {
	return t.d.RetractTable(key)
}

func (t *derivativeWindowWindowTransformation) Process(id execute.DatasetID, tbl flux.Table) error {
	builder, created := t.cache.TableBuilder(tbl.Key())
	if !created {
		return errors.Newf(codes.FailedPrecondition, "derivativeWindow found duplicate table with key: %v", tbl.Key())
	}

	cols := tbl.Cols()
	doDerivative := make([]*derivativeWindow, len(cols))
	timeIdx := -1
	for j, c := range cols {
		d := &derivativeWindow{
			col:         c,
			unit:        t.unit,
			nonNegative: t.nonNegative,
			window:      t.window,
		}
		if !execute.ContainsStr(t.columns, c.Label) {
			d.passthrough = true
		}

		if c.Label == t.timeCol {
			timeIdx = j
		}
		doDerivative[j] = d
	}
	if timeIdx < 0 {
		return errors.Newf(codes.FailedPrecondition, "no column %q exists", t.timeCol)
	}
	for j, d := range doDerivative {
		typ, err := d.Type()
		if err != nil {
			return err
		}
		c := flux.ColMeta{
			Label: cols[j].Label,
			Type:  typ,
		}
		if _, err := builder.AddCol(c); err != nil {
			return err
		}
	}
	var lastTs int64
	var bns execute.Bounds
	return tbl.Do(func(cr flux.ColReader) error {
		if cr.Len() == 0 {
			return nil
		}

		ts := cr.Times(timeIdx)
		if ts.NullN() > 0 {
			return errors.New(codes.FailedPrecondition, "derivativeWindow found null time in time column")
		}
		var index []int

		for i := 0; i < ts.Len(); i++ {
			if ts.IsNull(i) {
				continue
			}
			tt := ts.Value(i)
			if tt < lastTs {
				errors.New(codes.FailedPrecondition, "derivativeWindow found disorder timestamp")
			}

			ttt := execute.Time(tt)
			if bns.IsEmpty() {
				bns = t.window.GetEarliestBounds(ttt)
				index = append(index, i)
			} else if !bns.Contains(ttt) {
				index = append(index, i)
				for ; !bns.Contains(ttt); bns = bns.Shift(t.window.Every) {
				}
			}
			lastTs = tt
		}
		for j, d := range doDerivative {
			d.index = index
			vs := table.Values(cr, j)
			if err := d.Do(ts, vs, builder, j); err != nil {
				return err
			}
		}
		return nil
	})
}

func (t *derivativeWindowWindowTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *derivativeWindowWindowTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *derivativeWindowWindowTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}

const derivativeWindowUnsortedTimeErr = "derivativeWindow found out-of-order times in time column"

// derivativeWindow computes the derivativeWindow for an array.
type derivativeWindow struct {
	t           int64
	v           interface{}
	col         flux.ColMeta
	unit        float64
	passthrough bool
	nonNegative bool
	initialized bool
	index       []int
	window      execute.Window
	bns         execute.Bounds
	last        Last
}

// Type will return the type for this column given the input type.
func (d *derivativeWindow) Type() (flux.ColType, error) {
	if d.passthrough {
		return d.col.Type, nil
	}

	switch d.col.Type {
	case flux.TFloat, flux.TInt, flux.TUInt:
		// The above types are the only ones that support derivativeWindow.
		return flux.TFloat, nil
	default:
		// Everything else will fail.
		return flux.TInvalid, errors.Newf(codes.FailedPrecondition, "unsupported derivativeWindow column type %s:%s", d.col.Label, d.col.Type)
	}
}

// Do will compute the derivativeWindow for the given array using the times.
func (d *derivativeWindow) Do(ts *array.Int64, vs array.Interface, b execute.TableBuilder, j int) error {
	switch d.col.Type {
	case flux.TInt:
		return d.doInts(ts, vs.(*array.Int64), b, j)
	case flux.TUInt:
		return d.doUints(ts, vs.(*array.Uint64), b, j)
	case flux.TFloat:
		return d.doFloats(ts, vs.(*array.Float64), b, j)
	case flux.TString:
		return d.doStrings(ts, vs.(*array.Binary), b, j)
	case flux.TBool:
		return d.doBools(ts, vs.(*array.Boolean), b, j)
	case flux.TTime:
		return d.doTimes(ts, vs.(*array.Int64), b, j)
	}
	return errors.Newf(codes.Unimplemented, "derivativeWindow: column type %s is unimplemented", d.col.Type)
}

func (d *derivativeWindow) doInts(ts, vs *array.Int64, b execute.TableBuilder, j int) error {

	index := d.index
	// Initialize by reading the first value.
	if !d.initialized {
		i := index[0]
		d.t = ts.Value(i)
		if vs.IsValid(i) {
			d.v = vs.Value(i)
		}
		d.initialized = true
		index = index[1:]
	}

	// Process the rest of the rows.
	for _, i := range index {
		t := ts.Value(i)
		if t < d.t {
			return errors.New(codes.FailedPrecondition, derivativeWindowUnsortedTimeErr)
		} else if t == d.t {
			// If time did not increase with this row, ignore it.
			continue
		}

		// If we have been told to pass through the value, just do that.
		if d.passthrough {
			if vs.IsNull(i) {
				if err := b.AppendNil(j); err != nil {
					return err
				}
			} else {
				if err := b.AppendInt(j, vs.Value(i)); err != nil {
					return err
				}
			}
			d.t = t
			continue
		}

		// If the current value is nil, append nil and skip to the
		// next point. We do not modify the previous value when we
		// see null and we do not update the timestamp.
		if vs.IsNull(i) {
			if err := b.AppendNil(j); err != nil {
				return err
			}
			continue
		}

		// If we haven't yet seen a valid value, append nil and use
		// the current value as the previous for the next iteration.
		// to use the current value.
		if d.v == nil {
			if err := b.AppendNil(j); err != nil {
				return err
			}
			d.t, d.v = t, vs.Value(i)
			continue
		}

		// We have seen a valid value so retrieve it now.
		pv, cv := d.v.(int64), vs.Value(i)
		if d.nonNegative && pv > cv {
			// The previous value is greater than the current
			// value and non-negative was set.
			if err := b.AppendNil(j); err != nil {
				return err
			}
		} else {
			// Do the derivative.
			elapsed := float64(t-d.t) / d.unit
			diff := float64(cv - pv)
			if err := b.AppendFloat(j, diff/elapsed); err != nil {
				return err
			}
		}
		d.t, d.v = t, cv
	}
	return nil
}

func (d *derivativeWindow) doUints(ts *array.Int64, vs *array.Uint64, b execute.TableBuilder, j int) error {
	index := d.index
	// Initialize by reading the first value.
	if !d.initialized {
		i := index[0]
		d.t = ts.Value(i)
		if vs.IsValid(i) {
			d.v = vs.Value(i)
		}
		d.initialized = true
		index = index[1:]
	}

	// Process the rest of the rows.
	for _, i := range index {
		t := ts.Value(i)
		if t < d.t {
			return errors.New(codes.FailedPrecondition, derivativeUnsortedTimeErr)
		} else if t == d.t {
			// If time did not increase with this row, ignore it.
			continue
		}

		// If we have been told to pass through the value, just do that.
		if d.passthrough {
			if vs.IsNull(i) {
				if err := b.AppendNil(j); err != nil {
					return err
				}
			} else {
				if err := b.AppendUInt(j, vs.Value(i)); err != nil {
					return err
				}
			}
			d.t = t
			continue
		}

		// If the current value is nil, append nil and skip to the
		// next point. We do not modify the previous value when we
		// see null and we do not update the timestamp.
		if vs.IsNull(i) {
			if err := b.AppendNil(j); err != nil {
				return err
			}
			continue
		}

		// If we haven't yet seen a valid value, append nil and use
		// the current value as the previous for the next iteration.
		// to use the current value.
		if d.v == nil {
			if err := b.AppendNil(j); err != nil {
				return err
			}
			d.t, d.v = t, vs.Value(i)
			continue
		}

		// We have seen a valid value so retrieve it now.
		pv, cv := d.v.(uint64), vs.Value(i)
		if d.nonNegative && pv > cv {
			// The previous value is greater than the current
			// value and non-negative was set.
			if err := b.AppendNil(j); err != nil {
				return err
			}
		} else {
			// Do the derivative.
			elapsed := float64(t-d.t) / d.unit

			var diff float64
			if pv > cv {
				// Avoid wrapping on unsigned subtraction.
				diff = -float64(pv - cv)
			} else {
				diff = float64(cv - pv)
			}

			if err := b.AppendFloat(j, diff/elapsed); err != nil {
				return err
			}
		}
		d.t, d.v = t, cv
	}
	return nil
}

func (d *derivativeWindow) doFloats(ts *array.Int64, vs *array.Float64, b execute.TableBuilder, j int) error {
	index := d.index
	// Initialize by reading the first value.
	if !d.initialized {
		i := index[0]
		d.t = ts.Value(i)
		if vs.IsValid(i) {
			d.v = vs.Value(i)
		}
		d.initialized = true
		index = index[1:]
	}

	// Process the rest of the rows.
	for _, i := range index {
		t := ts.Value(i)
		if t < d.t {
			return errors.New(codes.FailedPrecondition, derivativeWindowUnsortedTimeErr)
		} else if t == d.t {
			// If time did not increase with this row, ignore it.
			continue
		}

		// If we have been told to pass through the value, just do that.
		if d.passthrough {
			if vs.IsNull(i) {
				if err := b.AppendNil(j); err != nil {
					return err
				}
			} else {
				if err := b.AppendFloat(j, vs.Value(i)); err != nil {
					return err
				}
			}
			d.t = t
			continue
		}

		// If the current value is nil, append nil and skip to the
		// next point. We do not modify the previous value when we
		// see null and we do not update the timestamp.
		if vs.IsNull(i) {
			if err := b.AppendNil(j); err != nil {
				return err
			}
			continue
		}

		// If we haven't yet seen a valid value, append nil and use
		// the current value as the previous for the next iteration.
		// to use the current value.
		if d.v == nil {
			if err := b.AppendNil(j); err != nil {
				return err
			}
			d.t, d.v = t, vs.Value(i)
			continue
		}

		// We have seen a valid value so retrieve it now.
		pv, cv := d.v.(float64), vs.Value(i)
		if d.nonNegative && pv > cv {
			// The previous value is greater than the current
			// value and non-negative was set.
			if err := b.AppendNil(j); err != nil {
				return err
			}
		} else {
			// Do the derivativeWindow.
			elapsed := float64(t-d.t) / d.unit
			diff := cv - pv
			if err := b.AppendFloat(j, diff/elapsed); err != nil {
				return err
			}
		}
		d.t, d.v = t, cv
	}
	return nil
}

func (d *derivativeWindow) doStrings(ts *array.Int64, vs *array.Binary, b execute.TableBuilder, j int) error {
	index := d.index
	// Initialize by reading the first value.
	if !d.initialized {
		i := index[0]
		d.t = ts.Value(i)
		if vs.IsValid(i) {
			d.v = vs.Value(i)
		}
		d.initialized = true
		index = index[1:]
	}

	// Process the rest of the rows.
	for _, i := range index {
		t := ts.Value(i)
		if t < d.t {
			return errors.New(codes.FailedPrecondition, derivativeWindowUnsortedTimeErr)
		} else if t == d.t {
			// If time did not increase with this row, ignore it.
			continue
		}

		if vs.IsNull(i) {
			if err := b.AppendNil(j); err != nil {
				return err
			}
		} else {
			if err := b.AppendString(j, vs.ValueString(i)); err != nil {
				return err
			}
		}
		d.t = t
	}
	return nil
}

func (d *derivativeWindow) doBools(ts *array.Int64, vs *array.Boolean, b execute.TableBuilder, j int) error {
	index := d.index
	// Initialize by reading the first value.
	if !d.initialized {
		i := index[0]
		d.t = ts.Value(i)
		if vs.IsValid(i) {
			d.v = vs.Value(i)
		}
		d.initialized = true
		index = index[1:]
	}

	// Process the rest of the rows.
	for _, i := range index {
		t := ts.Value(i)
		if t < d.t {
			return errors.New(codes.FailedPrecondition, derivativeWindowUnsortedTimeErr)
		} else if t == d.t {
			// If time did not increase with this row, ignore it.
			continue
		}

		if vs.IsNull(i) {
			if err := b.AppendNil(j); err != nil {
				return err
			}
		} else {
			if err := b.AppendBool(j, vs.Value(i)); err != nil {
				return err
			}
		}
		d.t = t
	}
	return nil
}

func (d *derivativeWindow) doTimes(ts, vs *array.Int64, b execute.TableBuilder, j int) error {
	index := d.index
	// Initialize by reading the first value.
	if !d.initialized {
		i := index[0]
		d.t = ts.Value(i)
		if vs.IsValid(i) {
			d.v = vs.Value(i)
		}
		d.initialized = true
		index = index[1:]
	}

	// Process the rest of the rows.
	for _, i := range index {
		t := ts.Value(i)
		if t < d.t {
			return errors.New(codes.FailedPrecondition, derivativeWindowUnsortedTimeErr)
		} else if t == d.t {
			// If time did not increase with this row, ignore it.
			continue
		}
		if vs.IsNull(i) {
			if err := b.AppendNil(j); err != nil {
				return err
			}
		} else {
			if err := b.AppendTime(j, execute.Time(d.t)); err != nil {
				return err
			}
		}
		d.t = t
	}
	return nil
}
