// UnionOp streams the left source then the right source. Column names from the right
// batch are rewritten to match the left so downstream operators see one consistent schema.
package exec

import (
	"context"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type UnionOp struct {
	Left  Operator
	Right Operator

	names    []string
	useRight bool
	state    operatorState
}

func (u *UnionOp) Open(ctx context.Context) error {
	prev := u.state
	if err := u.state.open(); err != nil {
		return err
	}
	if err := u.Left.Open(ctx); err != nil {
		u.state = prev
		return err
	}
	if err := u.Right.Open(ctx); err != nil {
		_ = u.Left.Close()
		u.state = prev
		return err
	}
	u.names = nil
	u.useRight = false
	return nil
}

func (u *UnionOp) Next() (vector.Batch, bool, error) {
	if err := u.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	for {
		src := u.Left
		if u.useRight {
			src = u.Right
		}
		batch, ok, err := src.Next()
		if err != nil {
			return vector.Batch{}, false, err
		}
		if !ok {
			if u.useRight {
				return vector.Batch{}, false, nil
			}
			u.useRight = true
			continue
		}
		if u.names == nil {
			u.names = make([]string, len(batch.Columns))
			for i, c := range batch.Columns {
				u.names[i] = c.Name
			}
		} else {
			for i := range batch.Columns {
				if i >= len(u.names) {
					break
				}
				batch.Columns[i].Name = u.names[i]
			}
		}
		return batch, true, nil
	}
}

func (u *UnionOp) Close() error {
	u.state.close()
	leftErr := u.Left.Close()
	rightErr := u.Right.Close()
	if leftErr != nil {
		return leftErr
	}
	return rightErr
}
