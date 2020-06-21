package flux

func (spec *Spec) DetermineParentsChildrenAndRoots() (parents, children map[OperationID][]*Operation, roots []*Operation, _ error) {
	return spec.determineParentsChildrenAndRoots()
}
func (spec *Spec) RemoveOperation(op *Operation) error {
	var ops []*Operation
	var edges []Edge
	//rm operations
	for _, o := range spec.Operations {
		if o.ID != op.ID {
			ops = append(ops, o)
		}
	}
	// rm edge
	c := spec.Children(op.ID)
	ps := spec.Parents(op.ID)

	for _, edge := range spec.Edges {
		if edge.Child != op.ID && edge.Parent != op.ID {
			edges = append(edges, edge)
		}
	}
	for _, p := range ps {
		for _, child := range c {
			edges = append(edges, NewEdge(p, child))
		}
	}
	spec.Operations = ops
	spec.Edges = edges
	if err := spec.Validate(); err != nil {
		return err
	}
	return nil
}
func (spec *Spec) AddOperationBetween(op *Operation, childId, parentId OperationID) error {
	var edges []Edge
	//add operations
	spec.Operations = append(spec.Operations, op)
	// rm old edge
	for _, edge := range spec.Edges {
		if edge.Child != childId && edge.Parent != parentId {
			edges = append(edges, edge)
		}
	}
	//add new edge
	edges = append(edges, Edge{Parent: parentId, Child: op.ID})
	edges = append(edges, Edge{Parent: op.ID, Child: childId})
	spec.Edges = edges
	if err := spec.Validate(); err != nil {
		return err
	}
	return nil
}
func NewEdge(parent *Operation, child *Operation) Edge {
	return Edge{Parent: parent.ID, Child: child.ID}
}
