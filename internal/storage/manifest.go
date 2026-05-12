package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const manifestFileName = "manifest.jsonl"

// manifestSegment is the on-disk JSONL record per segment. Meta carries only
// the fields needed to enumerate the segment and sanity-check it against the
// segment file's footer; the footer remains the source of truth for stats,
// blooms, dictionaries, and per-page metadata. Older manifests written with
// the full SegmentMeta inline still decode here: encoding/json silently drops
// the extra fields, so legacy databases keep loading without migration.
type manifestSegment struct {
	Path string                `json:"path"`
	Meta manifestSegmentSignature `json:"meta"`
}

type manifestSegmentSignature struct {
	ID       SegmentID
	Rows     uint32
	PageRows uint32
	Columns  []manifestColumnSignature
}

type manifestColumnSignature struct {
	Name string
	Type types.Type
}

func signatureFromMeta(meta SegmentMeta) manifestSegmentSignature {
	sig := manifestSegmentSignature{
		ID:       meta.ID,
		Rows:     meta.Rows,
		PageRows: meta.PageRows,
		Columns:  make([]manifestColumnSignature, len(meta.Columns)),
	}
	for i, col := range meta.Columns {
		sig.Columns[i] = manifestColumnSignature{Name: col.Name, Type: col.Type}
	}
	return sig
}

func appendManifest(dir string, segment storedSegment) (err error) {
	path := filepath.Join(dir, manifestFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			err = syncDir(dir)
		}
	}()
	if err := truncatePartialManifestTail(file); err != nil {
		return err
	}
	relPath, err := filepath.Rel(dir, segment.path)
	if err != nil {
		return err
	}
	record := manifestSegment{Path: filepath.ToSlash(relPath), Meta: signatureFromMeta(segment.meta)}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}

type manifestRecord struct {
	line   int
	record manifestSegment
}

func parseManifestRecords(data []byte) ([]manifestRecord, error) {
	complete := bytes.HasSuffix(data, []byte{'\n'})
	lines := bytes.Split(data, []byte{'\n'})

	type pendingLine struct {
		line int
		body []byte
	}
	pending := make([]pendingLine, 0, len(lines))
	for i, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if i == len(lines)-1 && !complete {
			break
		}
		pending = append(pending, pendingLine{line: i + 1, body: line})
	}
	if len(pending) == 0 {
		return nil, nil
	}

	records := make([]manifestRecord, len(pending))
	workers := min(runtime.GOMAXPROCS(0), len(pending))
	if workers < 1 {
		workers = 1
	}

	var next atomic.Int64
	var failed atomic.Bool
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	total := int64(len(pending))
	recordErr := func(err error) {
		errOnce.Do(func() { firstErr = err })
		failed.Store(true)
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if failed.Load() {
					return
				}
				i := next.Add(1) - 1
				if i >= total {
					return
				}
				p := pending[i]
				var record manifestSegment
				if err := json.Unmarshal(p.body, &record); err != nil {
					recordErr(fmt.Errorf("manifest line %d: %w", p.line, err))
					return
				}
				if record.Path == "" {
					recordErr(fmt.Errorf("manifest line %d: segment path is required", p.line))
					return
				}
				records[i] = manifestRecord{line: p.line, record: record}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return records, nil
}

func readManifest(dir string) ([]storedSegment, error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	records, err := parseManifestRecords(data)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	if shouldMigrateManifest(len(data), len(records)) {
		if err := rewriteManifestSlim(dir, records); err != nil {
			return nil, fmt.Errorf("migrate manifest: %w", err)
		}
	}

	segments := make([]storedSegment, len(records))
	workers := runtime.GOMAXPROCS(0)
	if workers > len(records) {
		workers = len(records)
	}
	if workers < 1 {
		workers = 1
	}

	var next atomic.Int64
	var failed atomic.Bool
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	total := int64(len(records))
	recordErr := func(err error) {
		errOnce.Do(func() { firstErr = err })
		failed.Store(true)
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if failed.Load() {
					return
				}
				i := next.Add(1) - 1
				if i >= total {
					return
				}
				rec := records[i]
				path := filepath.Join(dir, filepath.FromSlash(rec.record.Path))
				footer, size, err := ReadSegmentFooter(path)
				if err != nil {
					recordErr(err)
					return
				}
				if err := validateManifestSegmentMeta(rec.line, rec.record.Meta, footer); err != nil {
					recordErr(err)
					return
				}
				segments[i] = storedSegment{path: path, meta: footer, size: size, pageInfos: synthesizeSegmentPageInfos(footer)}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return segments, nil
}

func validateManifestSegmentMeta(line int, manifest manifestSegmentSignature, footer SegmentMeta) error {
	if footer.ID != manifest.ID {
		return fmt.Errorf("manifest line %d segment id %d does not match footer id %d", line, manifest.ID, footer.ID)
	}
	if footer.Rows != manifest.Rows {
		return fmt.Errorf("manifest line %d segment rows %d does not match footer rows %d", line, manifest.Rows, footer.Rows)
	}
	if footer.PageRows != manifest.PageRows {
		return fmt.Errorf("manifest line %d segment page rows %d does not match footer page rows %d", line, manifest.PageRows, footer.PageRows)
	}
	if len(footer.Columns) != len(manifest.Columns) {
		return fmt.Errorf("manifest line %d has %d columns, footer has %d", line, len(manifest.Columns), len(footer.Columns))
	}
	for i := range footer.Columns {
		manifestCol := manifest.Columns[i]
		footerCol := footer.Columns[i]
		if footerCol.Name != manifestCol.Name {
			return fmt.Errorf("manifest line %d column %d name %q does not match footer name %q", line, i, manifestCol.Name, footerCol.Name)
		}
		if footerCol.Type != manifestCol.Type {
			return fmt.Errorf("manifest line %d column %q type %s does not match footer type %s", line, manifestCol.Name, manifestCol.Type, footerCol.Type)
		}
	}
	return nil
}

// shouldMigrateManifest detects legacy manifests written with the full
// SegmentMeta inline. A slim record is on the order of a few hundred bytes;
// the legacy format weighed in at megabytes per record. Anything averaging
// more than 4 KiB per record is treated as legacy and rewritten.
func shouldMigrateManifest(onDiskBytes, recordCount int) bool {
	if recordCount == 0 {
		return false
	}
	return onDiskBytes/recordCount > 4096
}

func rewriteManifestSlim(dir string, records []manifestRecord) (err error) {
	path := filepath.Join(dir, manifestFileName)
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(tmpPath)
	}
	for _, rec := range records {
		data, marshalErr := json.Marshal(rec.record)
		if marshalErr != nil {
			cleanup()
			return marshalErr
		}
		data = append(data, '\n')
		if _, writeErr := file.Write(data); writeErr != nil {
			cleanup()
			return writeErr
		}
	}
	if syncErr := file.Sync(); syncErr != nil {
		cleanup()
		return syncErr
	}
	if closeErr := file.Close(); closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}
	if renameErr := os.Rename(tmpPath, path); renameErr != nil {
		_ = os.Remove(tmpPath)
		return renameErr
	}
	return syncDir(dir)
}

func truncatePartialManifestTail(file *os.File) error {
	data, err := os.ReadFile(file.Name())
	if err != nil {
		return err
	}
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return nil
	}
	if idx := bytes.LastIndexByte(data, '\n'); idx >= 0 {
		return file.Truncate(int64(idx + 1))
	}
	return file.Truncate(0)
}
