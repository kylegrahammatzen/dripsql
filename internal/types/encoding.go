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
	default:
		return fmt.Sprintf("encoding(%d)", e)
	}
}
