package plan

import (
	"errors"
	"github.com/influxdata/flux"
)

const StageKind = "stage"

func init() {
	rangeSignature := flux.FunctionSignature(
		nil,
		nil,
	)

	flux.RegisterPackageValue("universe", StageKind, flux.FunctionValue(StageKind, createStageOpSpec, rangeSignature))
	flux.RegisterOpSpec(StageKind, newStageOp)
	RegisterProcedureSpec(StageKind, createStageProcedureSpec, StageKind)

}
func createStageOpSpec(args flux.Arguments, a *flux.Administration) (flux.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	return &StageOperationSpec{}, nil
}
func newStageOp() flux.OperationSpec {
	return StageOperationSpec{}
}

type StageOperationSpec struct {
	Spec flux.Spec
}

func (s *StageOperationSpec) AddOperation(operation *flux.Operation) {
	o := *operation
	s.Spec.Operations = append(s.Spec.Operations, &o)
}
func (s *StageOperationSpec) AddEdge(e flux.Edge) {
	s.Spec.Edges = append(s.Spec.Edges, e)
}
func (StageOperationSpec) Kind() flux.OperationKind {
	return StageKind
}

type StageProcedureSpec struct {
	Spec flux.Spec
}

func (spec StageProcedureSpec) Cost(inStats []Statistics) (cost Cost, outStats Statistics) {
	return Cost{}, Statistics{}
}

func createStageProcedureSpec(qs flux.OperationSpec, pa Administration) (ProcedureSpec, error) {
	o := qs.(*StageOperationSpec)

	return &StageProcedureSpec{
		Spec: o.Spec,
	}, nil
}
func (spec StageProcedureSpec) Kind() ProcedureKind {
	return StageKind
}

func (spec StageProcedureSpec) Copy() ProcedureSpec {
	return &StageProcedureSpec{
		Spec: spec.Spec,
	}
}

type StagePlanner struct {
	timeRange *flux.Operation
}

type stageMarkVisitor struct {
	children map[flux.OperationID][]*flux.Operation
}

func (v *stageMarkVisitor) walk(op *flux.Operation, f func(p, n *flux.Operation)) {
	ops := v.children[op.ID]
	for _, ch := range ops {
		if IsPushDownOp(ch) {
			f(op, ch)
			v.walk(ch, f)
		}

	}

}

type visitor struct {
	relations map[flux.OperationID][]*flux.Operation
}

func (v *visitor) walk(operation *flux.Operation, f func(first, second *flux.Operation)) {
	for _, op := range v.relations[operation.ID] {
		f(operation, op)
		v.walk(op, f)
	}
}
func edge(parent *flux.Operation, child *flux.Operation) flux.Edge {
	return flux.Edge{Parent: parent.ID, Child: child.ID}
}

var enableStagePlan bool

func EnableStagePlan() {
	enableStagePlan = true
}
func PlanStage(spec *flux.Spec) (*flux.Spec, error) {
	if enableStagePlan {
		return StagePlanner{}.Plan(spec)
	}
	return spec, nil

}
func (sp StagePlanner) Plan(spec *flux.Spec) (*flux.Spec, error) {
	var err error
	spec, err = sp.setup(spec)
	if err != nil {
		return nil, err
	}
	spec, err = sp.plan(spec)
	if err != nil {
		return nil, err
	}
	return spec, nil
}
func (sp StagePlanner) setup(spec *flux.Spec) (*flux.Spec, error) {
	var stages []*flux.Operation

	spec.Walk(func(o *flux.Operation) error {
		if o.Spec.Kind() == StageKind {
			stages = append(stages, o)
		}
		return nil
	})
	parents, children, _, err := spec.DetermineParentsChildrenAndRoots()
	if err != nil {
		return nil, err
	}
	for _, root := range stages {
		stageSpec, ok := root.Spec.(*StageOperationSpec)
		stageSpec.Spec.Now = spec.Now
		if !ok {
			return nil, errors.New("not StageOperationSpec")
		}
		v := visitor{relations: parents}
		v.walk(root, func(p, n *flux.Operation) {
			stageSpec.AddOperation(n)
			if p.Spec.Kind() != StageKind {
				stageSpec.AddEdge(edge(n, p))
			}
		})
		if err := stageSpec.Spec.Validate(); err != nil {
			return nil, err
		}
		err := stageSpec.Spec.Walk(func(o *flux.Operation) error {
			if o.Spec.Kind() == "range" {
				sp.timeRange = o
			}
			return spec.RemoveOperation(o)
		})
		if err != nil {
			return nil, err
		}
		for _, child := range children[root.ID] {
			if err := spec.AddOperationBetween(sp.timeRange, child.ID, root.ID); err != nil {
				return nil, err
			}
		}
	}
	return spec, nil
}
func (sp StagePlanner) plan(spec *flux.Spec) (*flux.Spec, error) {
	_, children, roots, err := spec.DetermineParentsChildrenAndRoots()
	if err != nil {
		return nil, err
	}
	var markedOp []*flux.Operation
	v := stageMarkVisitor{
		children: children,
	}
	for _, root := range roots {
		if root.Spec.Kind() == StageKind {
			continue
		}
		stageSpec := &StageOperationSpec{
			Spec: flux.Spec{
				Now:       spec.Now,
				Resources: spec.Resources,
			},
		}
		stageSpec.AddOperation(root)
		v.walk(root, func(p, n *flux.Operation) {
			markedOp = append(markedOp, n)
			stageSpec.AddOperation(n)
			stageSpec.AddEdge(edge(p, n))
		})
		root.Spec = stageSpec
	}
	skipped := []flux.OperationKind{flux.OperationKind("group"), flux.OperationKind("filter"), flux.OperationKind("window")}

	for _, op := range markedOp {
		for _, skip := range skipped {
			if skip == op.Spec.Kind() {
				spec.RemoveOperation(op)
			}
		}
	}

	return spec, nil

}
