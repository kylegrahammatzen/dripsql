package codec

import "github.com/kylegrahammatzen/dripsql/internal/types"

// CandidateSet is the page-local codec cascade entry point. Today each
// candidate is a complete page codec; future layers like FSST can be added to
// the relevant set without teaching the segment writer about them.
type CandidateSet []Codec

func (c CandidateSet) Pick(v types.Vec) (PreparedEncoding, bool) {
	return PickSmallestPrepared(v, c...)
}

func TextCandidates() CandidateSet {
	return CandidateSet{Plain{}, Dictionary{}, Constant{}, Flate{}}
}

func FixedCandidates() CandidateSet {
	return CandidateSet{Plain{}, Constant{}, FORBitPack{}}
}
