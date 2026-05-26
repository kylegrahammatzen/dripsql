// SegmentIdentity is the per-segment self-description that lets a reader resolve
// columns by stable catalog identity rather than position. Stored as an optional
// sidecar section so existing dsv4 segments keep loading; absence means "legacy
// positional, fall back to ordinal mapping in the engine".
package storage

import (
	"encoding/binary"
	"fmt"
)

type SegmentIdentity struct {
	TableID          uint64
	SchemaGeneration uint64
	ColumnIDs        []uint64
}

const identitySidecarMagic = "DSID"

const sidecarSectionIdentity uint8 = 5

func encodeIdentitySidecar(id SegmentIdentity) []byte {
	if id.TableID == 0 && id.SchemaGeneration == 0 && len(id.ColumnIDs) == 0 {
		return nil
	}
	size := len(identitySidecarMagic) + 8 + 8 + 4 + 8*len(id.ColumnIDs)
	buf := make([]byte, 0, size)
	buf = append(buf, identitySidecarMagic...)
	buf = binary.LittleEndian.AppendUint64(buf, id.TableID)
	buf = binary.LittleEndian.AppendUint64(buf, id.SchemaGeneration)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(id.ColumnIDs)))
	for _, c := range id.ColumnIDs {
		buf = binary.LittleEndian.AppendUint64(buf, c)
	}
	return buf
}

func decodeIdentitySidecar(data []byte) (SegmentIdentity, error) {
	hdr := len(identitySidecarMagic) + 8 + 8 + 4
	if len(data) < hdr {
		return SegmentIdentity{}, fmt.Errorf("identity sidecar: short header (%d bytes)", len(data))
	}
	if string(data[:len(identitySidecarMagic)]) != identitySidecarMagic {
		return SegmentIdentity{}, fmt.Errorf("identity sidecar: bad magic")
	}
	pos := len(identitySidecarMagic)
	tableID := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	gen := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if pos+n*8 != len(data) {
		return SegmentIdentity{}, fmt.Errorf("identity sidecar: body length mismatch (have %d, want %d)", len(data)-pos, n*8)
	}
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
	}
	return SegmentIdentity{TableID: tableID, SchemaGeneration: gen, ColumnIDs: ids}, nil
}
