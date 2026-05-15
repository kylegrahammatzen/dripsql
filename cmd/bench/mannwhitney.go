// Mann-Whitney U two-sample rank-sum test. Returns the two-sided p-value via the normal approximation
// with continuity correction and tie-corrected variance. Used by bench -compare to flag regressions.
package main

import (
	"math"
	"sort"
)

func mannWhitneyU(a, b []float64) (u float64, p float64) {
	n1, n2 := len(a), len(b)
	if n1 == 0 || n2 == 0 {
		return 0, 1
	}
	type tagged struct {
		v   float64
		grp int
	}
	combined := make([]tagged, 0, n1+n2)
	for _, v := range a {
		combined = append(combined, tagged{v: v, grp: 0})
	}
	for _, v := range b {
		combined = append(combined, tagged{v: v, grp: 1})
	}
	sort.SliceStable(combined, func(i, j int) bool { return combined[i].v < combined[j].v })

	ranks := make([]float64, len(combined))
	var tieSum float64
	i := 0
	for i < len(combined) {
		j := i
		for j+1 < len(combined) && combined[j+1].v == combined[i].v {
			j++
		}
		avg := float64(i+j+2) / 2.0
		for k := i; k <= j; k++ {
			ranks[k] = avg
		}
		t := float64(j - i + 1)
		if t > 1 {
			tieSum += (t*t*t - t)
		}
		i = j + 1
	}

	var r1 float64
	for k, c := range combined {
		if c.grp == 0 {
			r1 += ranks[k]
		}
	}

	n := float64(n1 + n2)
	u1 := r1 - float64(n1)*float64(n1+1)/2.0
	u2 := float64(n1)*float64(n2) - u1
	u = math.Min(u1, u2)

	mean := float64(n1) * float64(n2) / 2.0
	stdVar := (float64(n1) * float64(n2) / 12.0) * ((n + 1) - tieSum/(n*(n-1)))
	if stdVar <= 0 {
		return u, 1
	}
	sd := math.Sqrt(stdVar)
	z := (math.Abs(u-mean) - 0.5) / sd
	if z < 0 {
		z = 0
	}
	p = 2 * (1 - normalCDF(z))
	return u, p
}

func normalCDF(z float64) float64 {
	return 0.5 * (1 + math.Erf(z/math.Sqrt2))
}
