// Single-file sidecar container: one tmp+fsync+rename+dirsync writes all three
// per-segment sidecars (dict-hist, int-filter, num-sum) as tagged sections.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const sidecarContainerMagic = "DSCV1"

const (
	sidecarSectionDictHist  uint8 = 1
	sidecarSectionIntFilter uint8 = 2
	sidecarSectionNumSum    uint8 = 3
	sidecarSectionVarBloom  uint8 = 4
)

const sidecarSectionHeaderSize = 1 + 4

// EncodeSidecarContainer serializes sections to a single byte buffer suitable for
// embedding inline at the end of a segment file. Empty sections are skipped.
// Returns nil when every section is empty.
func EncodeSidecarContainer(sections map[uint8][]byte) []byte {
	live := make([]uint8, 0, len(sections))
	total := 0
	for tag, body := range sections {
		if len(body) == 0 {
			continue
		}
		live = append(live, tag)
		total += sidecarSectionHeaderSize + len(body)
	}
	if len(live) == 0 {
		return nil
	}
	hdr := len(sidecarContainerMagic) + 1
	buf := make([]byte, 0, hdr+total)
	buf = append(buf, sidecarContainerMagic...)
	buf = append(buf, uint8(len(live)))
	for _, tag := range live {
		body := sections[tag]
		buf = append(buf, tag)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(body)))
		buf = append(buf, body...)
	}
	return buf
}

// DecodeSidecarContainer parses bytes previously produced by EncodeSidecarContainer.
// Empty input returns (nil, nil). Bad magic or truncated section is an error.
func DecodeSidecarContainer(data []byte) (map[uint8][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	hdr := len(sidecarContainerMagic) + 1
	if len(data) < hdr || string(data[:len(sidecarContainerMagic)]) != sidecarContainerMagic {
		return nil, errors.New("sidecar container: bad magic")
	}
	count := int(data[len(sidecarContainerMagic)])
	pos := hdr
	out := make(map[uint8][]byte, count)
	for range count {
		if pos+sidecarSectionHeaderSize > len(data) {
			return nil, fmt.Errorf("sidecar container: header truncated at pos %d", pos)
		}
		tag := data[pos]
		bodyLen := int(binary.LittleEndian.Uint32(data[pos+1 : pos+5]))
		pos += sidecarSectionHeaderSize
		if pos+bodyLen > len(data) {
			return nil, fmt.Errorf("sidecar container: body truncated for tag %d", tag)
		}
		out[tag] = data[pos : pos+bodyLen]
		pos += bodyLen
	}
	if pos != len(data) {
		return nil, errors.New("sidecar container: trailing bytes")
	}
	return out, nil
}
