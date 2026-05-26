// Encoding tags a Vec with its physical buffer layout where the wire byte equals the iota value.
// EncInvalid is a binder sentinel rejected at the wire boundary.
package schema

type Encoding uint8

const (
	EncInvalid Encoding = iota
	EncPlain
	EncDict
	EncConstant
	EncSequence
	EncFOR
	EncDelta
	EncFlate
	EncZstd
	EncALP
	EncALPRD
	EncFSST
	EncPcodec
)

const encMax = EncPcodec

func (e Encoding) String() string {
	switch e {
	case EncInvalid:
		return "invalid"
	case EncPlain:
		return "plain"
	case EncDict:
		return "dict"
	case EncConstant:
		return "const"
	case EncSequence:
		return "seq"
	case EncFOR:
		return "for"
	case EncDelta:
		return "delta"
	case EncFlate:
		return "flate"
	case EncZstd:
		return "zstd"
	case EncALP:
		return "alp"
	case EncALPRD:
		return "alp-rd"
	case EncFSST:
		return "fsst"
	case EncPcodec:
		return "pcodec"
	}
	return "encoding(?)"
}

func (e Encoding) Wire() uint8 {
	if e == EncInvalid {
		panic("EncInvalid is a hint sentinel and cannot be encoded to wire")
	}
	if e > encMax {
		panic("unknown encoding cannot be encoded to wire")
	}
	return uint8(e)
}

// Wire bytes never carry the auto sentinel and must fall within the registered enum range.
func (e Encoding) Valid() bool {
	return e != EncInvalid && e <= encMax
}

// ParseEncoding rejects the invalid sentinel and accepts the canonical short name plus DDL aliases for back-compat with existing schemas.
func ParseEncoding(name string) (Encoding, bool) {
	switch NormalizeName(name) {
	case "plain", "flat":
		return EncPlain, true
	case "dict", "dictionary":
		return EncDict, true
	case "const", "constant":
		return EncConstant, true
	case "seq", "sequence":
		return EncSequence, true
	case "for", "for+bitpack":
		return EncFOR, true
	case "delta", "delta+bitpack":
		return EncDelta, true
	case "flate":
		return EncFlate, true
	case "zstd":
		return EncZstd, true
	case "alp":
		return EncALP, true
	case "alp-rd":
		return EncALPRD, true
	case "fsst":
		return EncFSST, true
	case "pcodec":
		return EncPcodec, true
	}
	return EncInvalid, false
}
