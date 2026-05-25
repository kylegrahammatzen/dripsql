// Post-bind fusion pass that collapses stacked operators into equivalent single nodes.
// Currently merges RelFilter(RelFilter(x, p1), p2) into RelFilter(x, AND(p1, p2)).
package sql

func fusePlan(root *Rel) *Rel {
	if root == nil {
		return nil
	}
	for i, in := range root.Inputs {
		root.Inputs[i] = fusePlan(in)
	}
	if root.Op == RelFilter && len(root.Inputs) == 1 && root.Inputs[0] != nil && root.Inputs[0].Op == RelFilter {
		child := root.Inputs[0]
		combined := BoundExpr{
			Op:   ExprAnd,
			Type: root.Predicate.Type,
			Args: []BoundExpr{child.Predicate, root.Predicate},
		}
		root = &Rel{
			Op:        RelFilter,
			Outputs:   root.Outputs,
			Inputs:    child.Inputs,
			Predicate: combined,
		}
	}
	return root
}
