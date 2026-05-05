package table

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Appender batches segment writes and persists the manifest once on Close.
// It is not safe for concurrent use.
type Appender struct {
	table         *Table
	file          *os.File
	dataPath      string
	offset        int64
	initialOffset int64
	initialNext   uint64
	initialLen    int
	dirty         bool
	closed        bool
}

// Append writes batch as one immutable table segment and records it in the manifest.
func (t *Table) Append(batch vector.Batch) error {
	appender, err := t.NewAppender()
	if err != nil {
		return err
	}
	if err := appender.Append(batch); err != nil {
		_ = appender.Close()
		return err
	}
	return appender.Close()
}

// NewAppender opens a bulk append session. Call Close to persist the manifest.
func (t *Table) NewAppender() (*Appender, error) {
	dataPath := filepath.Join(t.dir, dataFile)
	file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	offset := t.dataEnd()
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() < offset {
		_ = file.Close()
		return nil, fmt.Errorf("table data file is %d bytes, manifest requires at least %d", info.Size(), offset)
	}
	if info.Size() > offset {
		if err := file.Truncate(offset); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Appender{
		table:         t,
		file:          file,
		dataPath:      dataPath,
		offset:        offset,
		initialOffset: offset,
		initialNext:   t.manifest.NextSegmentID,
		initialLen:    len(t.manifest.Segments),
	}, nil
}

func (t *Table) dataEnd() int64 {
	var offset int64
	for _, segment := range t.manifest.Segments {
		end := segment.Offset + segment.Bytes
		if end > offset {
			offset = end
		}
	}
	return offset
}

func readColumnRanges(data []byte) ([]ColumnRange, error) {
	ranges, err := storage.SegmentColumnPayloadRanges(data)
	if err != nil {
		return nil, err
	}
	out := make([]ColumnRange, 0, len(ranges))
	for _, columnRange := range ranges {
		out = append(out, ColumnRange{Name: columnRange.Name, Offset: columnRange.Offset, Bytes: columnRange.Bytes})
	}
	return out, nil
}

// Append writes batch as one immutable segment in this append session.
func (a *Appender) Append(batch vector.Batch) error {
	if a == nil || a.table == nil || a.file == nil || a.closed {
		return fmt.Errorf("table appender is not open")
	}
	t := a.table
	if err := t.validateBatch(batch); err != nil {
		return err
	}

	id := t.manifest.NextSegmentID

	var buf bytes.Buffer
	stats, err := storage.WriteSegment(&buf, batch)
	if err != nil {
		return err
	}
	columnRanges, err := readColumnRanges(buf.Bytes())
	if err != nil {
		return err
	}
	if n, err := a.file.Write(buf.Bytes()); err != nil {
		_ = a.file.Truncate(a.offset)
		_, _ = a.file.Seek(a.offset, io.SeekStart)
		return err
	} else if n != buf.Len() {
		_ = a.file.Truncate(a.offset)
		_, _ = a.file.Seek(a.offset, io.SeekStart)
		return io.ErrShortWrite
	}

	entry := Segment{ID: id, Offset: a.offset, Rows: stats.Rows, Bytes: int64(buf.Len()), Stats: stats, Columns: columnRanges}
	t.manifest.NextSegmentID++
	t.manifest.Segments = append(t.manifest.Segments, entry)
	a.offset += int64(buf.Len())
	a.dirty = true
	return nil
}

// Close persists the manifest and closes the append session.
func (a *Appender) Close() error {
	if a == nil || a.closed {
		return nil
	}
	a.closed = true
	t := a.table
	file := a.file
	a.file = nil
	if file == nil {
		return nil
	}
	if t == nil {
		return file.Close()
	}

	if err := file.Close(); err != nil {
		if rollbackErr := a.rollback(); rollbackErr != nil {
			return fmt.Errorf("close data file: %w; rollback data file: %v", err, rollbackErr)
		}
		return err
	}
	if !a.dirty {
		return nil
	}

	if err := t.writeManifest(); err != nil {
		if rollbackErr := a.rollback(); rollbackErr != nil {
			return fmt.Errorf("write manifest: %w; rollback data file: %v", err, rollbackErr)
		}
		return err
	}
	return nil
}

func (a *Appender) rollback() error {
	if a == nil || a.table == nil {
		return nil
	}
	t := a.table
	if a.initialLen <= len(t.manifest.Segments) {
		t.manifest.NextSegmentID = a.initialNext
		t.manifest.Segments = t.manifest.Segments[:a.initialLen]
	}
	if a.dataPath == "" {
		return nil
	}
	return os.Truncate(a.dataPath, a.initialOffset)
}
