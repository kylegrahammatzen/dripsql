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
	return CandidateSet{Plain{}, Dictionary{}, Constant{}, Flate{}, Zstd{}}
}

// TextCandidatesFor returns the text cascade narrowed by the table compression
// policy. Structural codecs (Plain, Dictionary, Constant) are always present
// since they are not CPU-heavy; Flate and Zstd are gated by the policy.
func TextCandidatesFor(policy types.CompressionPolicy) CandidateSet {
	set := CandidateSet{Plain{}, Dictionary{}, Constant{}}
	if policy.AllowsFlate() {
		set = append(set, Flate{})
	}
	if policy.AllowsZstd() {
		set = append(set, Zstd{})
	}
	return set
}

func FixedCandidates() CandidateSet {
	return CandidateSet{Plain{}, Constant{}, FORBitPack{}, DeltaBitPack{}}
}
