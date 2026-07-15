// Column-at-a-time evaluation for numeric arithmetic projections.
// Unsupported shapes return ok=false so the row-by-row eval in project.go stays authoritative.
package exec

import (
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// numOperand is one evaluated subtree, either a scalar constant or a full column slice.
type numOperand struct {
	isFloat bool
	isConst bool
	ci      int64
	cf      float64
	i       []int64
	f       []float64
	valid   vector.Validity
}

func (o numOperand) atI(row int) int64 {
	if o.isConst {
		return o.ci
	}
	return o.i[row]
}

func (o numOperand) atF(row int) float64 {
	if o.isConst {
		return o.cf
	}
	return o.f[row]
}

func vecEvalArith(batch vector.Batch, expr sql.BoundExpr, sel *vector.SelectionMask, rows int, vk vector.VecKind) (vector.Vec, vector.Validity, bool, error) {
	if vk != vector.VecInt64 && vk != vector.VecFloat64 {
		return vector.Vec{}, nil, false, nil
	}
	opnd, ok, err := vecEvalNode(batch, expr, sel, rows)
	if err != nil || !ok {
		return vector.Vec{}, nil, ok, err
	}
	out := vector.NewVec(vk, rows)
	switch {
	case vk == vector.VecFloat64 && opnd.isFloat:
		dst := out.F64()
		if opnd.isConst {
			for i := range dst {
				dst[i] = opnd.cf
			}
		} else {
			copy(dst, opnd.f)
		}
	case vk == vector.VecFloat64:
		dst := out.F64()
		if opnd.isConst {
			for i := range dst {
				dst[i] = float64(opnd.ci)
			}
		} else {
			for i, v := range opnd.i {
				dst[i] = float64(v)
			}
		}
	case vk == vector.VecInt64 && !opnd.isFloat:
		dst := out.I64()
		if opnd.isConst {
			for i := range dst {
				dst[i] = opnd.ci
			}
		} else {
			copy(dst, opnd.i)
		}
	default:
		return vector.Vec{}, nil, false, nil
	}
	return out, opnd.valid, true, nil
}

func vecEvalNode(batch vector.Batch, expr sql.BoundExpr, sel *vector.SelectionMask, rows int) (numOperand, bool, error) {
	switch expr.Op {
	case sql.ExprLiteral:
		switch v := expr.Literal.(type) {
		case int64:
			return numOperand{isConst: true, ci: v}, true, nil
		case float64:
			return numOperand{isConst: true, isFloat: true, cf: v}, true, nil
		}
		return numOperand{}, false, nil
	case sql.ExprColumn:
		col, ok := batch.ColumnByName(expr.Column)
		if !ok || int(col.V.Len) < rows {
			return numOperand{}, false, nil
		}
		switch col.V.Kind {
		case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
			return numOperand{i: col.V.I64()[:rows], valid: col.V.Valid}, true, nil
		case vector.VecFloat64:
			return numOperand{isFloat: true, f: col.V.F64()[:rows], valid: col.V.Valid}, true, nil
		}
		return numOperand{}, false, nil
	case sql.ExprAdd, sql.ExprSubtract, sql.ExprMultiply, sql.ExprDivide, sql.ExprModulo, sql.ExprIntDivide:
		if len(expr.Args) != 2 {
			return numOperand{}, false, nil
		}
		l, ok, err := vecEvalNode(batch, expr.Args[0], sel, rows)
		if !ok || err != nil {
			return numOperand{}, ok, err
		}
		r, ok, err := vecEvalNode(batch, expr.Args[1], sel, rows)
		if !ok || err != nil {
			return numOperand{}, ok, err
		}
		return vecApplyArith(expr.Op, l, r, sel, rows)
	}
	return numOperand{}, false, nil
}

func vecApplyArith(op sql.ExprOp, l, r numOperand, sel *vector.SelectionMask, rows int) (numOperand, bool, error) {
	// Constant folding goes through the scalar helper so semantics stay identical to row eval.
	if l.isConst && r.isConst {
		var lv, rv any
		if l.isFloat {
			lv = l.cf
		} else {
			lv = l.ci
		}
		if r.isFloat {
			rv = r.cf
		} else {
			rv = r.ci
		}
		res, err := arithmetic(op, lv, rv)
		if err != nil {
			return numOperand{}, false, err
		}
		switch x := res.(type) {
		case int64:
			return numOperand{isConst: true, ci: x}, true, nil
		case float64:
			return numOperand{isConst: true, isFloat: true, cf: x}, true, nil
		}
		return numOperand{}, false, nil
	}
	valid := mergeValidity(l.valid, r.valid, rows)
	if l.isFloat || r.isFloat {
		if op == sql.ExprModulo || op == sql.ExprIntDivide {
			return numOperand{}, false, nil
		}
		lf, rf := promoteFloat(l, rows), promoteFloat(r, rows)
		out := make([]float64, rows)
		switch op {
		case sql.ExprAdd:
			switch {
			case lf.isConst:
				c := lf.cf
				for i, v := range rf.f {
					out[i] = c + v
				}
			case rf.isConst:
				c := rf.cf
				for i, v := range lf.f {
					out[i] = v + c
				}
			default:
				for i := range out {
					out[i] = lf.f[i] + rf.f[i]
				}
			}
		case sql.ExprSubtract:
			switch {
			case lf.isConst:
				c := lf.cf
				for i, v := range rf.f {
					out[i] = c - v
				}
			case rf.isConst:
				c := rf.cf
				for i, v := range lf.f {
					out[i] = v - c
				}
			default:
				for i := range out {
					out[i] = lf.f[i] - rf.f[i]
				}
			}
		case sql.ExprMultiply:
			switch {
			case lf.isConst:
				c := lf.cf
				for i, v := range rf.f {
					out[i] = c * v
				}
			case rf.isConst:
				c := rf.cf
				for i, v := range lf.f {
					out[i] = v * c
				}
			default:
				for i := range out {
					out[i] = lf.f[i] * rf.f[i]
				}
			}
		case sql.ExprDivide:
			// Division only runs on selected valid rows so a masked-out zero divisor cannot raise.
			var loopErr error
			sel.IterSet(func(row int) {
				if loopErr != nil {
					return
				}
				if valid != nil && !valid.IsValid(row) {
					return
				}
				q, err := vector.DivFloat(lf.atF(row), rf.atF(row))
				if err != nil {
					loopErr = err
					return
				}
				out[row] = q
			})
			if loopErr != nil {
				return numOperand{}, false, loopErr
			}
		}
		return numOperand{isFloat: true, f: out, valid: valid}, true, nil
	}
	out := make([]int64, rows)
	switch op {
	case sql.ExprAdd:
		switch {
		case l.isConst:
			c := l.ci
			for i, v := range r.i {
				out[i] = c + v
			}
		case r.isConst:
			c := r.ci
			for i, v := range l.i {
				out[i] = v + c
			}
		default:
			for i := range out {
				out[i] = l.i[i] + r.i[i]
			}
		}
	case sql.ExprSubtract:
		switch {
		case l.isConst:
			c := l.ci
			for i, v := range r.i {
				out[i] = c - v
			}
		case r.isConst:
			c := r.ci
			for i, v := range l.i {
				out[i] = v - c
			}
		default:
			for i := range out {
				out[i] = l.i[i] - r.i[i]
			}
		}
	case sql.ExprMultiply:
		switch {
		case l.isConst:
			c := l.ci
			for i, v := range r.i {
				out[i] = c * v
			}
		case r.isConst:
			c := r.ci
			for i, v := range l.i {
				out[i] = v * c
			}
		default:
			for i := range out {
				out[i] = l.i[i] * r.i[i]
			}
		}
	case sql.ExprDivide, sql.ExprIntDivide, sql.ExprModulo:
		mod := op == sql.ExprModulo
		var loopErr error
		sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			if valid != nil && !valid.IsValid(row) {
				return
			}
			var q int64
			var err error
			if mod {
				q, err = vector.ModInt(l.atI(row), r.atI(row))
			} else {
				q, err = vector.DivInt(l.atI(row), r.atI(row))
			}
			if err != nil {
				loopErr = err
				return
			}
			out[row] = q
		})
		if loopErr != nil {
			return numOperand{}, false, loopErr
		}
	}
	return numOperand{i: out, valid: valid}, true, nil
}

func promoteFloat(o numOperand, rows int) numOperand {
	if o.isFloat {
		return o
	}
	if o.isConst {
		return numOperand{isFloat: true, isConst: true, cf: float64(o.ci)}
	}
	f := make([]float64, rows)
	for i, v := range o.i {
		f[i] = float64(v)
	}
	return numOperand{isFloat: true, f: f, valid: o.valid}
}

func mergeValidity(a, b vector.Validity, rows int) vector.Validity {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	out := make(vector.Validity, vector.ValidityWords(rows))
	copy(out, a)
	for i := range out {
		out[i] &= b[i]
	}
	return out
}
