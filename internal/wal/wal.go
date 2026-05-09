package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
)

const (
	frameMagic         uint32 = 0x4c415744 // "DWAL"
	frameVersion       uint8  = 1
	frameReservedStart        = 6
	frameReservedEnd          = 8
	frameHeaderLen            = 20
	frameMinLen               = frameHeaderLen + 4
)

// WAL frame layout:
// [0:4] magic, [4] version, [5] type, [6:8] reserved, [8:16] LSN,
// [16:20] payload length, payload, checksum.

type LSN uint64

type RecordType uint8

const (
	RecordUnknown RecordType = iota
	RecordInsert
	RecordCommit
)

type Record struct {
	LSN     LSN
	Type    RecordType
	Payload []byte
}

func EncodeRecord(record Record) ([]byte, error) {
	if record.LSN == 0 {
		return nil, fmt.Errorf("wal record LSN must be non-zero")
	}
	if len(record.Payload) > int(^uint32(0)) {
		return nil, fmt.Errorf("wal payload too large")
	}
	payloadLen := len(record.Payload)
	frame := make([]byte, frameHeaderLen+payloadLen+4)
	binary.LittleEndian.PutUint32(frame[0:4], frameMagic)
	frame[4] = frameVersion
	frame[5] = byte(record.Type)
	// frame[6:8] is reserved and remains zeroed.
	binary.LittleEndian.PutUint64(frame[8:16], uint64(record.LSN))
	binary.LittleEndian.PutUint32(frame[16:20], uint32(payloadLen))
	copy(frame[frameHeaderLen:frameHeaderLen+payloadLen], record.Payload)
	// Magic is a sync marker checked separately; checksum covers version through payload.
	checksum := crc32.ChecksumIEEE(frame[4 : frameHeaderLen+payloadLen])
	binary.LittleEndian.PutUint32(frame[frameHeaderLen+payloadLen:], checksum)
	return frame, nil
}

func DecodeRecord(frame []byte) (Record, error) {
	if len(frame) < frameMinLen {
		return Record{}, fmt.Errorf("wal frame too short")
	}
	if binary.LittleEndian.Uint32(frame[0:4]) != frameMagic {
		return Record{}, fmt.Errorf("wal frame magic mismatch")
	}
	if frame[4] != frameVersion {
		return Record{}, fmt.Errorf("wal frame version %d is unsupported", frame[4])
	}
	payloadLen := int(binary.LittleEndian.Uint32(frame[16:20]))
	wantLen := frameHeaderLen + payloadLen + 4
	if len(frame) != wantLen {
		return Record{}, fmt.Errorf("wal frame length mismatch: have %d want %d", len(frame), wantLen)
	}
	wantChecksum := binary.LittleEndian.Uint32(frame[frameHeaderLen+payloadLen:])
	gotChecksum := crc32.ChecksumIEEE(frame[4 : frameHeaderLen+payloadLen])
	if gotChecksum != wantChecksum {
		return Record{}, fmt.Errorf("wal frame checksum mismatch")
	}
	record := Record{
		LSN:  LSN(binary.LittleEndian.Uint64(frame[8:16])),
		Type: RecordType(frame[5]),
	}
	if record.LSN == 0 {
		// EncodeRecord refuses this; keep DecodeRecord defensive for external frames.
		return Record{}, fmt.Errorf("wal record LSN must be non-zero")
	}
	record.Payload = bytes.Clone(frame[frameHeaderLen : frameHeaderLen+payloadLen])
	return record, nil
}

func DecodeFrames(data []byte) ([]Record, error) {
	if len(data) == 0 {
		return nil, nil
	}
	records := make([]Record, 0, 16)
	var lastLSN LSN
	for len(data) != 0 {
		if len(data) < frameMinLen {
			// Crash recovery tolerates a partial tail only after at least one complete frame.
			if hasCompleteRecord := len(records) != 0; !hasCompleteRecord {
				return nil, fmt.Errorf("wal truncated frame")
			}
			break
		}
		payloadLen := int(binary.LittleEndian.Uint32(data[16:20]))
		frameLen := frameHeaderLen + payloadLen + 4
		if frameLen < frameMinLen || len(data) < frameLen {
			if hasCompleteRecord := len(records) != 0; !hasCompleteRecord {
				return nil, fmt.Errorf("wal truncated frame")
			}
			break
		}
		record, err := DecodeRecord(data[:frameLen])
		if err != nil {
			return nil, err
		}
		// Insert and commit records share an LSN, so equal LSNs are valid here.
		if lastLSN != 0 && record.LSN < lastLSN {
			return nil, fmt.Errorf("wal LSN %d is less than previous LSN %d", record.LSN, lastLSN)
		}
		lastLSN = record.LSN
		records = append(records, record)
		data = data[frameLen:]
	}
	return records, nil
}

func Append(path string, record Record) error {
	frame, err := EncodeRecord(record)
	if err != nil {
		return err
	}
	created := false
	if _, err := os.Stat(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		created = true
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	if _, err := file.Write(frame); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if created {
		return syncDir(filepath.Dir(path))
	}
	return nil
}

// ReadAll loads the whole WAL; table-local WAL files are expected to stay small.
func ReadAll(path string) ([]Record, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return DecodeFrames(data)
}

func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
