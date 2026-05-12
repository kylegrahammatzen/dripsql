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
	default:
		return fmt.Sprintf("encoding(%d)", e)
	}
}
