// Scalar kernels shared by predicate eval, sort comparators, and aggregate accumulators.
// Generic over cmp.Ordered so int16/int32/int64 share one implementation.
package vector

import (
	"bytes"
	"cmp"
	"fmt"
)

func CmpOrdered[T cmp.Ordered](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func CmpBytes(a, b []byte) int { return bytes.Compare(a, b) }

func CmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	default:
		return -1
	}
}

type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

func AddInt[T Integer](a, b T) T { return a + b }
func SubInt[T Integer](a, b T) T { return a - b }
func MulInt[T Integer](a, b T) T { return a * b }

func DivInt[T Integer](a, b T) (T, error) {
	if b == 0 {
		return 0, fmt.Errorf("divide by zero")
	}
	return a / b, nil
}

func ModInt[T Integer](a, b T) (T, error) {
	if b == 0 {
		return 0, fmt.Errorf("mod by zero")
	}
	return a % b, nil
}

func AddFloat(a, b float64) float64 { return a + b }
func SubFloat(a, b float64) float64 { return a - b }
func MulFloat(a, b float64) float64 { return a * b }

func DivFloat(a, b float64) (float64, error) {
	if b == 0 {
		return 0, fmt.Errorf("divide by zero")
	}
	return a / b, nil
}
