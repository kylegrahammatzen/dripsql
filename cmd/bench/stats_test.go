package main

import (
	"math"
	"testing"
)

func TestMannWhitneyDetectsShift(t *testing.T) {
	a := []int64{100, 102, 98, 101, 99, 103, 97, 100}
	b := []int64{120, 122, 118, 121, 119, 123, 117, 120}
	p, ok := mannWhitneyP(a, b)
	if !ok {
		t.Fatal("expected mannWhitneyP to succeed on equal-sized samples")
	}
	if p >= 0.05 {
		t.Fatalf("p = %f, expected < 0.05 for a clearly shifted distribution", p)
	}
}

func TestMannWhitneyAcceptsNoise(t *testing.T) {
	a := []int64{100, 102, 98, 101, 99, 103, 97, 100}
	b := []int64{101, 99, 102, 100, 98, 103, 100, 101}
	p, ok := mannWhitneyP(a, b)
	if !ok {
		t.Fatal("expected mannWhitneyP to succeed on overlapping samples")
	}
	if p < 0.05 {
		t.Fatalf("p = %f, expected >= 0.05 for indistinguishable samples", p)
	}
}

func TestMannWhitneyEmptyFallback(t *testing.T) {
	if _, ok := mannWhitneyP(nil, []int64{1, 2, 3}); ok {
		t.Fatal("empty side should signal no test result")
	}
	if _, ok := mannWhitneyP([]int64{1, 2, 3}, nil); ok {
		t.Fatal("empty side should signal no test result")
	}
}

func TestMannWhitneyIdenticalSamples(t *testing.T) {
	a := []int64{10, 10, 10, 10, 10}
	b := []int64{10, 10, 10, 10, 10}
	p, ok := mannWhitneyP(a, b)
	if !ok {
		t.Fatal("expected ok on identical samples")
	}
	if math.Abs(p-1) > 1e-9 {
		t.Fatalf("p = %f, expected exactly 1 for tied samples", p)
	}
}
