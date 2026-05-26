// Encoding tags a Vec with its physical buffer layout where the wire byte equals the iota value.
// EncodingAuto is a binder sentinel rejected at the wire boundary.
package schema

type Encoding uint8

const (
	EncodingAuto Encoding = iota
	EncodingFlat
	EncodingDictionary
	EncodingConstant
	EncodingSequence
	EncodingFORBitPack
	EncodingDeltaBitPack
	EncodingFlate
	EncodingZstd
	EncodingALP
	EncodingALPRD
	EncodingFSST
	EncodingPcodec
)

const encodingMax = EncodingPcodec

func (e Encoding) String() string {
	switch e {
	case EncodingAuto:
		return "auto"
	case EncodingFlat:
		return "flat"
	case EncodingDictionary:
		return "dictionary"
	case EncodingConstant:
		return "constant"
	case EncodingSequence:
		return "sequence"
	case EncodingFORBitPack:
		return "for+bitpack"
	case EncodingDeltaBitPack:
		return "delta+bitpack"
	case EncodingFlate:
		return "flate"
	case EncodingZstd:
		return "zstd"
	case EncodingALP:
		return "alp"
	case EncodingALPRD:
		return "alp-rd"
	case EncodingFSST:
		return "fsst"
	case EncodingPcodec:
		return "pcodec"
	}
	return "encoding(?)"
}

func (e Encoding) Wire() uint8 {
	if e == EncodingAuto {
		panic("EncodingAuto is a hint sentinel and cannot be encoded to wire")
	}
	if e > encodingMax {
		panic("unknown encoding cannot be encoded to wire")
	}
	return uint8(e)
}

// Wire bytes never carry the auto sentinel and must fall within the registered enum range.
func (e Encoding) Valid() bool {
	return e != EncodingAuto && e <= encodingMax
}

// ParseEncoding rejects "auto" so DDL cannot smuggle the sentinel onto the wire and accepts "plain" as an alias for "flat".
func ParseEncoding(name string) (Encoding, bool) {
	switch NormalizeName(name) {
	case "plain", "flat":
		return EncodingFlat, true
	case "dictionary":
		return EncodingDictionary, true
	case "constant":
		return EncodingConstant, true
	case "sequence":
		return EncodingSequence, true
	case "for+bitpack":
		return EncodingFORBitPack, true
	case "delta+bitpack":
		return EncodingDeltaBitPack, true
	case "flate":
		return EncodingFlate, true
	case "zstd":
		return EncodingZstd, true
	case "alp":
		return EncodingALP, true
	case "alp-rd":
		return EncodingALPRD, true
	case "fsst":
		return EncodingFSST, true
	case "pcodec":
		return EncodingPcodec, true
	}
	return EncodingAuto, false
}
