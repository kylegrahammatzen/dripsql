package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const manifestFileName = "manifest.jsonl"

type manifestSegment struct {
	Path string      `json:"path"`
	Meta SegmentMeta `json:"meta"`
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
	record := manifestSegment{Path: filepath.ToSlash(relPath), Meta: segment.meta}
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
	complete := bytes.HasSuffix(data, []byte{'\n'})
	lines := bytes.Split(data, []byte{'\n'})
	segments := make([]storedSegment, 0, len(lines))
	for i, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if i == len(lines)-1 && !complete {
			break
		}
		var record manifestSegment
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("manifest line %d: %w", i+1, err)
		}
		if record.Path == "" {
			return nil, fmt.Errorf("manifest line %d: segment path is required", i+1)
		}
		path := filepath.Join(dir, filepath.FromSlash(record.Path))
		footer, err := ReadSegmentFooter(path)
		if err != nil {
			return nil, err
		}
		if err := validateManifestSegmentMeta(i+1, record.Meta, footer); err != nil {
			return nil, err
		}
		segments = append(segments, storedSegment{path: path, meta: footer})
	}
	return segments, nil
}

func validateManifestSegmentMeta(line int, manifest SegmentMeta, footer SegmentMeta) error {
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
