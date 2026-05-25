// Verifies fusePlan collapses stacked RelFilter nodes into a single AND-merged filter.
// Also asserts non-filter trees pass through unchanged.
package sql

import "testing"

func TestFusePlan_MergesStackedFilters(t *testing.T) {
	scan := &Rel{Op: RelScan, Outputs: nil}
	p1 := BoundExpr{Op: ExprColumn, Column: "a"}
	p2 := BoundExpr{Op: ExprColumn, Column: "b"}
	inner := &Rel{Op: RelFilter, Outputs: scan.Outputs, Inputs: []*Rel{scan}, Predicate: p1}
	outer := &Rel{Op: RelFilter, Outputs: scan.Outputs, Inputs: []*Rel{inner}, Predicate: p2}
	got := fusePlan(outer)
	if got.Op != RelFilter {
		t.Fatalf("want RelFilter root, got %v", got.Op)
	}
	if len(got.Inputs) != 1 || got.Inputs[0].Op != RelScan {
		t.Fatalf("want single RelScan input, got %+v", got.Inputs)
	}
	if got.Predicate.Op != ExprAnd || len(got.Predicate.Args) != 2 {
		t.Fatalf("want AND predicate with 2 args, got %+v", got.Predicate)
	}
	if got.Predicate.Args[0].Column != "a" || got.Predicate.Args[1].Column != "b" {
		t.Fatalf("predicate order wrong: %+v", got.Predicate.Args)
	}
}

func TestFusePlan_NoChangeOnSingleFilter(t *testing.T) {
	scan := &Rel{Op: RelScan}
	pred := BoundExpr{Op: ExprColumn, Column: "x"}
	f := &Rel{Op: RelFilter, Inputs: []*Rel{scan}, Predicate: pred}
	got := fusePlan(f)
	if got.Predicate.Op == ExprAnd {
		t.Fatal("single filter must not become AND")
	}
}
