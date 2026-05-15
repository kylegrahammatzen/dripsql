// Encoding tags a Vec with its physical buffer layout.
// One encodingTable drives String, Wire, and EncodingFromWire so the wire mapping has a single source of truth.
package types

import "fmt"

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
)

type encodingInfo struct {
	name string
	wire uint8
}

var encodingTable = [...]encodingInfo{
	EncodingAuto:         {"auto", 0},
	EncodingFlat:         {"flat", 1},
	EncodingDictionary:   {"dictionary", 2},
	EncodingConstant:     {"constant", 3},
	EncodingSequence:     {"sequence", 4},
	EncodingFORBitPack:   {"for+bitpack", 5},
	EncodingDeltaBitPack: {"delta+bitpack", 6},
	EncodingFlate:        {"flate", 7},
	EncodingZstd:         {"zstd", 8},
}

func (e Encoding) String() string {
	if int(e) < len(encodingTable) {
		return encodingTable[e].name
	}
	return fmt.Sprintf("encoding(%d)", e)
}

func (e Encoding) Wire() uint8 {
	if e == EncodingAuto {
		panic("EncodingAuto is a hint sentinel and cannot be encoded to wire")
	}
	if int(e) >= len(encodingTable) {
		panic(fmt.Sprintf("unknown encoding %d cannot be encoded to wire", e))
	}
	return encodingTable[e].wire
}

var wireToEncoding = func() [256]Encoding {
	var m [256]Encoding
	for i, info := range encodingTable {
		if Encoding(i) == EncodingAuto {
			continue
		}
		m[info.wire] = Encoding(i)
	}
	return m
}()

func EncodingFromWire(b uint8) (Encoding, bool) {
	if b == 0 {
		return EncodingAuto, false
	}
	e := wireToEncoding[b]
	if e == EncodingAuto {
		return EncodingAuto, false
	}
	return e, true
}
