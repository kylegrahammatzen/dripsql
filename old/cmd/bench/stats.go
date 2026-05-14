package main

import (
	"math"
	"sort"
)

// mannWhitneyP returns the two-sided p-value for the Mann-Whitney U
// test on two samples. The test is rank-based, so it detects shifts in
// distribution location without assuming normality, which is the right
// shape for warm/cold bench timings where outliers are common. Uses the
// normal approximation, fine for n>=8 per group (we run >=5).
//
// Returns (p, true) when both samples are non-empty; otherwise (0, false)
// to let the caller fall back to a simpler delta check.
func mannWhitneyP(a, b []int64) (float64, bool) {
	if len(a) == 0 || len(b) == 0 {
		return 0, false
	}
	n1, n2 := len(a), len(b)
	combined := make([]ranked, 0, n1+n2)
	for _, v := range a {
		combined = append(combined, ranked{value: v, group: 0})
	}
	for _, v := range b {
		combined = append(combined, ranked{value: v, group: 1})
	}
	sort.Slice(combined, func(i, j int) bool { return combined[i].value < combined[j].value })
	assignRanks(combined)
	var r1 float64
	for _, r := range combined {
		if r.group == 0 {
			r1 += r.rank
		}
	}
	u1 := r1 - float64(n1*(n1+1))/2
	u2 := float64(n1*n2) - u1
	u := math.Min(u1, u2)
	mu := float64(n1*n2) / 2
	sigma := math.Sqrt(float64(n1*n2*(n1+n2+1)) / 12)
	if sigma == 0 {
		return 1, true
	}
	z := (u - mu) / sigma
	p := math.Erfc(math.Abs(z) / math.Sqrt2)
	return p, true
}

type ranked struct {
	value int64
	rank  float64
	group int
}

// assignRanks fills in rank fields, averaging across ties so the
// rank-sum is unbiased.
func assignRanks(items []ranked) {
	i := 0
	for i < len(items) {
		j := i + 1
		for j < len(items) && items[j].value == items[i].value {
			j++
		}
		avg := float64(i+j+1) / 2
		for k := i; k < j; k++ {
			items[k].rank = avg
		}
		i = j
	}
}

// median returns the middle sample (or the lower of the two middles) for
// a sorted-or-unsorted slice. Used by the baseline diff to show a
// distribution-shape-aware center alongside avg.
func median(samples []int64) int64 {
	if len(samples) == 0 {
		return 0
	}
	ordered := append([]int64(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered[len(ordered)/2]
}
