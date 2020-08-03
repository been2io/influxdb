package plan

import "fmt"

const ConcurrentTransformerKind = "concurrentTransformer"


type ConcurrentPlanner struct {
	spec  *Spec
	index int
}

func newConcurrentPlanner(spec *Spec) *ConcurrentPlanner {
	return &ConcurrentPlanner{spec: spec}
}
func (p *ConcurrentPlanner) Plan() {
	p.spec.BottomUpWalk(func(node Node) error {
		if len(node.Predecessors()) == 0 {
			for _, succ := range node.Successors() {
				p.makeConcurrent(succ, p.spec.Resources.ConcurrencyQuota)
			}

		}
		return nil
	})
}

func (p *ConcurrentPlanner) makeConcurrent(node Node, concurrency int) error {
	if node.Kind() == generatedYieldKind{
		return nil
	}
	new := &PhysicalPlanNode{
		id:   NodeID(fmt.Sprintf("concurrent %v", p.index)),
		Spec: &ShardConcurrentSpec{},
	}
	p.index++
	insertNodeBefore(node, new)
	for _, succ := range new.Successors() {
		for i := 0; i < concurrency-1; i++ {
			cloned := p.clone(succ)
			new.AddSuccessors(cloned)
			cloned.AddPredecessors(new)
			succ = cloned
		}

	}
	return nil
}
func (p *ConcurrentPlanner) clone(old Node) Node {
	new := old.ShallowCopy()
	new.ClearPredecessors()
	new.ClearSuccessors()
	if len(old.Successors()) == 0 {
		p.spec.Roots[new] = struct{}{}
	}
	for _, succ := range old.Successors() {
		n := p.clone(succ)
		new.AddSuccessors(n)
		n.AddPredecessors(new)
	}
	return new
}
func insertNodeBefore(oldNode, newNode Node) {
	newNode.ClearPredecessors()
	newNode.ClearSuccessors()
	for i, pred := range oldNode.Predecessors() {
		for _, succ := range pred.Successors() {
			if succ == oldNode {
				pred.Successors()[i] = newNode
			}
		}
	}
	newNode.AddPredecessors(oldNode.Predecessors()...)
	oldNode.ClearPredecessors()
	newNode.AddSuccessors(oldNode)
	oldNode.AddPredecessors(newNode)
}

type ShardConcurrentSpec struct {
}

func (s *ShardConcurrentSpec) Kind() ProcedureKind {
	return ConcurrentTransformerKind
}

func (s *ShardConcurrentSpec) Copy() ProcedureSpec {
	return s
}

func (s *ShardConcurrentSpec) Cost(inStats []Statistics) (cost Cost, outStats Statistics) {
	panic("implement me")
}
