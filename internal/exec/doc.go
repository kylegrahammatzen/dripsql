// Package exec turns a bound sql.Plan into a chain of pull-based vectorized operators.
// Each Next call returns (batch, selection, ok, err). ok=false signals EOF.
package exec
