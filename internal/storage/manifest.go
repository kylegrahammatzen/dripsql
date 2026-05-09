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

func appendManifest(dir string, meta SegmentMeta) error {
	path := filepath.Join(dir, manifestFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := truncatePartialManifestTail(file); err != nil {
		_ = file.Close()
		return err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		_ = file.Close()
		return err
	}
	data = append(data, '\n')
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncDir(dir)
}

func truncatePartialManifestTail(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return nil
	}
	var last [1]byte
	if _, err := file.ReadAt(last[:], info.Size()-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	const chunkSize = 4096
	buf := make([]byte, chunkSize)
	for offset := info.Size(); offset > 0; {
		start := offset - chunkSize
		if start < 0 {
			start = 0
		}
		n := int(offset - start)
		if _, err := file.ReadAt(buf[:n], start); err != nil {
			return err
		}
		if idx := bytes.LastIndexByte(buf[:n], '\n'); idx >= 0 {
			return file.Truncate(start + int64(idx) + 1)
		}
		offset = start
	}
	return file.Truncate(0)
}

func readManifest(dir string) ([]SegmentMeta, error) {
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
	segments := make([]SegmentMeta, 0, len(lines))
	for i, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if i == len(lines)-1 && !complete {
			break
		}
		var meta SegmentMeta
		if err := json.Unmarshal(line, &meta); err != nil {
			return nil, fmt.Errorf("manifest line %d: %w", i+1, err)
		}
		segments = append(segments, meta)
	}
	return segments, nil
}
