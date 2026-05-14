package types

import "fmt"

type Encoding uint8

const (
	EncodingFlat Encoding = iota
	EncodingDictionary
	EncodingConstant
	EncodingSequence
	EncodingFORBitPack
	EncodingFlate
	EncodingDeltaBitPack
	EncodingZstd
	// EncodingAuto is a hint sentinel used by ColumnHints.PreferredCodec to mean
	// "let the cascade pick". It must never be written to disk; Wire panics if
	// asked to serialize it, and FromWire surfaces it from the reserved wire
	// byte 0 only for hint round-trip.
	EncodingAuto
)

func (e Encoding) String() string {
	switch e {
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
	case EncodingFlate:
		return "flate"
	case EncodingDeltaBitPack:
		return "delta+bitpack"
	case EncodingZstd:
		return "zstd"
	case EncodingAuto:
		return "auto"
	default:
		return fmt.Sprintf("encoding(%d)", e)
	}
}

// Wire returns the on-disk byte for e. The mapping is identity today so v4
// segments stay readable; the codec-hint reshuffle (wire byte 0 reserved for
// the auto sentinel, Plain shifting to wire byte 1) is a future format bump.
// EncodingAuto panics because it is a hint sentinel and must never be written.
func (e Encoding) Wire() byte {
	if e == EncodingAuto {
		panic("EncodingAuto is a hint sentinel and cannot be encoded to wire")
	}
	return byte(e)
}

// EncodingFromWire reverses Wire. Returns (encoding, true) for any byte that
// names a known on-disk codec; (EncodingAuto, false) for unknown bytes so the
// reader can surface a clear "unsupported encoding" error rather than silently
// reinterpreting it.
func EncodingFromWire(b byte) (Encoding, bool) {
	e := Encoding(b)
	if e == EncodingAuto {
		return EncodingAuto, false
	}
	switch e {
	case EncodingFlat, EncodingDictionary, EncodingConstant, EncodingSequence,
		EncodingFORBitPack, EncodingFlate, EncodingDeltaBitPack, EncodingZstd:
		return e, true
	}
	return EncodingAuto, false
}
