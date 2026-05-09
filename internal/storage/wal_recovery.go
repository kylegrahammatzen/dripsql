package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/wal"
)

const walLogFileName = "wal.log"

type walInsertPayload struct {
	// Keep insert WAL extensible without changing the top-level record envelope.
	Segment SegmentMeta `json:"segment"`
}

func appendWALInsert(dir string, table catalog.TableDef, lsn wal.LSN, meta SegmentMeta) error {
	if table.ID == 0 {
		return fmt.Errorf("table ID is required")
	}
	if lsn == 0 {
		return fmt.Errorf("wal LSN is required")
	}
	payload, err := json.Marshal(walInsertPayload{Segment: meta})
	if err != nil {
		return err
	}
	return wal.Append(filepath.Join(dir, walLogFileName), wal.Record{LSN: lsn, Type: wal.RecordInsert, Payload: payload})
}

func recoverTableFromWAL(state *tableState) error {
	if state == nil {
		return fmt.Errorf("table state is nil")
	}
	pending := make(map[wal.LSN]walInsertPayload)
	committed := make(map[wal.LSN]walInsertPayload)
	committedLSN := make([]wal.LSN, 0)

	segmentsByID := make(map[SegmentID]struct{}, len(state.segments))
	for _, segment := range state.segments {
		segmentsByID[segment.ID] = struct{}{}
	}
	for _, segment := range state.segments {
		if segment.ID >= state.nextSegmentID {
			state.nextSegmentID = segment.ID + 1
		}
	}

	records, err := wal.ReadAll(filepath.Join(state.dir, walLogFileName))
	if err != nil {
		return err
	}
	maxLSN := wal.LSN(0)
	for _, record := range records {
		if record.LSN > maxLSN {
			maxLSN = record.LSN
		}
		switch record.Type {
		case wal.RecordInsert:
			var payload walInsertPayload
			if err := json.Unmarshal(record.Payload, &payload); err != nil {
				return fmt.Errorf("wal insert %d: %w", record.LSN, err)
			}
			if payload.Segment.TableID != state.table.ID {
				return fmt.Errorf("wal insert %d has table ID %d, want %d", record.LSN, payload.Segment.TableID, state.table.ID)
			}
			if payload.Segment.Path == "" {
				return fmt.Errorf("wal insert %d has empty segment path", record.LSN)
			}
			if filepath.IsAbs(payload.Segment.Path) {
				return fmt.Errorf("wal insert %d has absolute segment path %q", record.LSN, payload.Segment.Path)
			}
			if _, ok := pending[record.LSN]; ok {
				return fmt.Errorf("wal insert %d is duplicated", record.LSN)
			}
			if _, ok := committed[record.LSN]; ok {
				return fmt.Errorf("wal insert %d appears after commit", record.LSN)
			}
			if payload.Segment.ID >= state.nextSegmentID {
				state.nextSegmentID = payload.Segment.ID + 1
			}
			pending[record.LSN] = payload
		case wal.RecordCommit:
			if _, ok := committed[record.LSN]; ok {
				return fmt.Errorf("wal commit %d is duplicated", record.LSN)
			}
			payload, ok := pending[record.LSN]
			if !ok {
				return fmt.Errorf("wal commit %d has no matching insert", record.LSN)
			}
			committed[record.LSN] = payload
			committedLSN = append(committedLSN, record.LSN)
			delete(pending, record.LSN)
		case wal.RecordUnknown:
			return fmt.Errorf("wal record %d has unsupported type", record.LSN)
		default:
			return fmt.Errorf("wal record %d has unsupported type %d", record.LSN, record.Type)
		}
	}

	sort.Slice(committedLSN, func(i, j int) bool { return committedLSN[i] < committedLSN[j] })
	if len(pending) != 0 {
		firstPending := wal.LSN(0)
		for lsn := range pending {
			if firstPending == 0 || lsn < firstPending {
				firstPending = lsn
			}
		}
		return fmt.Errorf("wal insert %d has no matching commit", firstPending)
	}
	for _, lsn := range committedLSN {
		segment := committed[lsn].Segment
		if _, ok := segmentsByID[segment.ID]; ok {
			continue
		}
		footer, err := readSegmentFooter(segment.AbsPath(state.dir))
		if err != nil {
			return fmt.Errorf("wal lsn %d segment %d footer: %w", lsn, segment.ID, err)
		}
		if err := verifyRecoveredSegment(segment, footer, lsn); err != nil {
			return err
		}
		// Persist replayed segments to the manifest so the next reopen does not re-replay them.
		if err := appendManifest(state.dir, footer); err != nil {
			return fmt.Errorf("wal lsn %d backfill manifest: %w", lsn, err)
		}
		state.segments = append(state.segments, footer)
		segmentsByID[footer.ID] = struct{}{}
		if footer.ID >= state.nextSegmentID {
			state.nextSegmentID = footer.ID + 1
		}
	}
	if err := reserveExistingSegmentIDs(state); err != nil {
		return err
	}

	state.nextLSN = maxLSN + 1

	// All committed records are now reflected in the manifest; the WAL is redundant
	// for them and can be truncated. Idempotent on crash mid-truncate.
	if len(records) > 0 {
		if err := truncateWAL(filepath.Join(state.dir, walLogFileName)); err != nil {
			return err
		}
	}

	return nil
}

func truncateWAL(path string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func reserveExistingSegmentIDs(state *tableState) error {
	entries, err := os.ReadDir(filepath.Join(state.dir, "segments"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".dseg") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(name, ".dseg"), 10, 64)
		if err != nil {
			continue
		}
		if SegmentID(id) >= state.nextSegmentID {
			state.nextSegmentID = SegmentID(id) + 1
		}
	}
	return nil
}

func verifyRecoveredSegment(recorded, footer SegmentMeta, lsn wal.LSN) error {
	if recorded.ID != footer.ID {
		return fmt.Errorf("wal lsn %d: segment ID %d does not match footer %d", lsn, recorded.ID, footer.ID)
	}
	if recorded.TableID != footer.TableID {
		return fmt.Errorf("wal lsn %d: segment table ID %d does not match footer %d", lsn, recorded.TableID, footer.TableID)
	}
	if recorded.SchemaVersion != footer.SchemaVersion {
		return fmt.Errorf("wal lsn %d: segment schema version %d does not match footer %d", lsn, recorded.SchemaVersion, footer.SchemaVersion)
	}
	if recorded.Path != footer.Path {
		return fmt.Errorf("wal lsn %d: segment path %q does not match footer %q", lsn, recorded.Path, footer.Path)
	}
	if recorded.Rows != footer.Rows {
		return fmt.Errorf("wal lsn %d: segment rows %d does not match footer %d", lsn, recorded.Rows, footer.Rows)
	}
	if recorded.PageRows != footer.PageRows {
		return fmt.Errorf("wal lsn %d: segment page rows %d does not match footer %d", lsn, recorded.PageRows, footer.PageRows)
	}
	return nil
}
