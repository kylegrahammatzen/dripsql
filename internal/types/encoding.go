package types

import "fmt"

// Encoding tags a Vec with the physical layout of its data buffer.
type Encoding uint8

const (
	// EncodingAuto is a hint sentinel; it never goes to disk.
	EncodingAuto Encoding = iota
	EncodingFlat
	EncodingDictionary
	EncodingConstant
	EncodingSequence
	EncodingFORBitPack
	EncodingDeltaBitPack
	EncodingFlate
	EncodingZstd
)

var encodingNames = [...]string{
	EncodingAuto:         "auto",
	EncodingFlat:         "flat",
	EncodingDictionary:   "dictionary",
	EncodingConstant:     "constant",
	EncodingSequence:     "sequence",
	EncodingFORBitPack:   "for+bitpack",
	EncodingDeltaBitPack: "delta+bitpack",
	EncodingFlate:        "flate",
	EncodingZstd:         "zstd",
}

func (e Encoding) String() string {
	if int(e) < len(encodingNames) {
		if name := encodingNames[e]; name != "" {
			return name
		}
	}
	return fmt.Sprintf("encoding(%d)", e)
}

// Wire returns the on-disk byte for e and panics on EncodingAuto.
func (e Encoding) Wire() uint8 {
	if e == EncodingAuto {
		panic("EncodingAuto is a hint sentinel and cannot be encoded to wire")
	}
	return uint8(e)
}

// EncodingFromWire reverses Wire and reports false for unknown bytes.
func EncodingFromWire(b uint8) (Encoding, bool) {
	e := Encoding(b)
	switch e {
	case EncodingAuto, EncodingFlat, EncodingDictionary, EncodingConstant,
		EncodingSequence, EncodingFORBitPack, EncodingDeltaBitPack,
		EncodingFlate, EncodingZstd:
		return e, true
	}
	return EncodingAuto, false
}
