package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeRecordRejectsZeroLSN(t *testing.T) {
	if _, err := EncodeRecord(Record{Type: RecordInsert, Payload: []byte("payload")}); err == nil {
		t.Fatalf("EncodeRecord error = nil, want zero LSN error")
	}
}

func TestEncodeDecodeRecordRoundTrip(t *testing.T) {
	record := Record{LSN: 7, Type: RecordInsert, Payload: []byte("hello")}
	frame, err := EncodeRecord(record)
	if err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}
	decoded, err := DecodeRecord(frame)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if decoded.LSN != record.LSN || decoded.Type != record.Type || string(decoded.Payload) != string(record.Payload) {
		t.Fatalf("decoded = %#v, want %#v", decoded, record)
	}
}

func TestEncodeDecodeNilPayloadCommitRoundTrip(t *testing.T) {
	record := Record{LSN: 8, Type: RecordCommit}
	frame, err := EncodeRecord(record)
	if err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}
	decoded, err := DecodeRecord(frame)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	if decoded.LSN != record.LSN || decoded.Type != record.Type || len(decoded.Payload) != 0 {
		t.Fatalf("decoded = %#v, want empty-payload commit %#v", decoded, record)
	}
}

func TestDecodeRecordRejectsZeroLSN(t *testing.T) {
	frame, err := EncodeRecord(Record{LSN: 1, Type: RecordInsert, Payload: []byte("payload")})
	if err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}
	binary.LittleEndian.PutUint64(frame[8:16], 0)
	checksum := crc32.ChecksumIEEE(frame[4 : len(frame)-4])
	binary.LittleEndian.PutUint32(frame[len(frame)-4:], checksum)
	if _, err := DecodeRecord(frame); err == nil {
		t.Fatalf("DecodeRecord error = nil, want zero LSN error")
	}
}

func TestDecodeRecordRejectsChecksumMismatch(t *testing.T) {
	frame, err := EncodeRecord(Record{LSN: 1, Type: RecordCommit, Payload: []byte("abc")})
	if err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}
	frame[len(frame)-1] ^= 0xff
	if _, err := DecodeRecord(frame); err == nil {
		t.Fatalf("DecodeRecord error = nil, want checksum mismatch")
	}
}

func TestDecodeRecordRejectsPayloadChecksumMismatch(t *testing.T) {
	frame, err := EncodeRecord(Record{LSN: 1, Type: RecordCommit, Payload: []byte("abc")})
	if err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}
	frame[frameHeaderLen] ^= 0xff
	if _, err := DecodeRecord(frame); err == nil {
		t.Fatalf("DecodeRecord error = nil, want checksum mismatch")
	}
}

func TestDecodeFramesRejectsTruncatedTail(t *testing.T) {
	frame, err := EncodeRecord(Record{LSN: 1, Type: RecordInsert, Payload: []byte("payload")})
	if err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}
	truncated := frame[:len(frame)-2]
	if _, err := DecodeFrames(truncated); err == nil {
		t.Fatalf("DecodeFrames error = nil, want truncated frame error")
	}
}

func TestDecodeFramesAllowsTrailingBytesAfterCompleteFrame(t *testing.T) {
	frame, err := EncodeRecord(Record{LSN: 1, Type: RecordInsert, Payload: []byte("payload")})
	if err != nil {
		t.Fatalf("EncodeRecord: %v", err)
	}
	data := append(frame, 0xff)
	records, err := DecodeFrames(data)
	if err != nil {
		t.Fatalf("DecodeFrames: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records len = %d, want 1", len(records))
	}
}

func TestDecodeFramesRejectsOutOfOrderLSN(t *testing.T) {
	frame1, err := EncodeRecord(Record{LSN: 2, Type: RecordInsert, Payload: []byte("a")})
	if err != nil {
		t.Fatalf("EncodeRecord first: %v", err)
	}
	frame2, err := EncodeRecord(Record{LSN: 1, Type: RecordInsert, Payload: []byte("b")})
	if err != nil {
		t.Fatalf("EncodeRecord second: %v", err)
	}
	data := append(frame1, frame2...)
	if _, err := DecodeFrames(data); err == nil {
		t.Fatalf("DecodeFrames error = nil, want out-of-order LSN error")
	}
}

func TestDecodeFramesAllowsPairedDuplicateLSN(t *testing.T) {
	// Recovery validation checks transaction ordering; DecodeFrames only enforces monotonic LSNs.
	frame1, err := EncodeRecord(Record{LSN: 1, Type: RecordInsert, Payload: []byte("insert")})
	if err != nil {
		t.Fatalf("EncodeRecord first: %v", err)
	}
	frame2, err := EncodeRecord(Record{LSN: 1, Type: RecordCommit, Payload: []byte("commit")})
	if err != nil {
		t.Fatalf("EncodeRecord second: %v", err)
	}
	records, err := DecodeFrames(append(frame1, frame2...))
	if err != nil {
		t.Fatalf("DecodeFrames: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records len = %d, want 2", len(records))
	}
	if records[0].LSN != 1 || records[1].LSN != 1 {
		t.Fatalf("records LSNs = %d,%d, want 1,1", records[0].LSN, records[1].LSN)
	}
}

func TestAppendAndReadAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	if err := Append(path, Record{LSN: 1, Type: RecordInsert, Payload: []byte("one")}); err != nil {
		t.Fatalf("Append first: %v", err)
	}
	if err := Append(path, Record{LSN: 2, Type: RecordCommit, Payload: []byte("two")}); err != nil {
		t.Fatalf("Append second: %v", err)
	}
	records, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records len = %d, want 2", len(records))
	}
	if records[0].LSN != 1 || records[1].LSN != 2 {
		t.Fatalf("records LSNs = %d,%d, want 1,2", records[0].LSN, records[1].LSN)
	}
	if string(records[0].Payload) != "one" || string(records[1].Payload) != "two" {
		t.Fatalf("payloads = %q,%q", records[0].Payload, records[1].Payload)
	}
}

func TestReadAllMissingFileReturnsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.log")
	records, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("records len = %d, want 0", len(records))
	}
}

func TestAppendCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	if err := Append(path, Record{LSN: 1, Type: RecordInsert, Payload: []byte("x")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Stat: %v", err)
	}
	records, err := ReadAll(path)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(records) != 1 || records[0].LSN != 1 || records[0].Type != RecordInsert || string(records[0].Payload) != "x" {
		t.Fatalf("records = %#v, want appended record", records)
	}
}
