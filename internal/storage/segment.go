package storage

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

import (
	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const segmentMagic = "DRIPSEG1"

func writeSegment(ctx context.Context, root string, table catalog.TableDef, batch vector.Batch, id SegmentID) (SegmentMeta, error) {
	return writeSegmentBatches(ctx, root, table, []vector.Batch{batch}, id)
}

func writeSegmentBatches(ctx context.Context, root string, table catalog.TableDef, batches []vector.Batch, id SegmentID) (SegmentMeta, error) {
	if err := ctx.Err(); err != nil {
		return SegmentMeta{}, err
	}
	relPath := filepath.ToSlash(filepath.Join("segments", fmt.Sprintf("%016d.dseg", id)))
	finalPath := filepath.Join(tableDir(root, table), relPath)
	tmpPath := finalPath + ".tmp"

	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return SegmentMeta{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = file.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := file.WriteString(segmentMagic); err != nil {
		return SegmentMeta{}, err
	}
	offset := uint64(len(segmentMagic))

	meta := SegmentMeta{
		ID:            id,
		TableID:       table.ID,
		SchemaVersion: table.Version,
		Path:          relPath,
		Rows:          totalRows(batches),
		PageRows:      uint32(DefaultPageRows),
		Columns:       make([]ColumnMeta, 0, len(table.Columns)),
	}
	pageCount := totalPages(batches)

	for colIndex, tableCol := range table.Columns {
		colMeta := ColumnMeta{
			ColumnID:   tableCol.ID,
			Name:       tableCol.Name,
			Type:       tableCol.Type,
			EnumLabels: append([]string(nil), tableCol.Labels...),
			Codec:      CodecPlain,
			Rows:       meta.Rows,
			Pages:      make([]PageMeta, 0, pageCount),
		}
		rowStart := 0
		var firstCodec Codec
		mixedCodec := false
		for _, batch := range batches {
			batchCol := batch.Columns[colIndex]
			for start := 0; start < batch.Len; start += DefaultPageRows {
				rows := min(DefaultPageRows, batch.Len-start)
				payload, pageMeta, err := encodeColumnPage(batchCol, start, rows)
				if err != nil {
					return SegmentMeta{}, fmt.Errorf("column %q: %w", tableCol.Name, err)
				}
				pageMeta.RowStart = uint32(rowStart + start)
				pageMeta.Offset = offset
				pageMeta.Length = uint64(len(payload))
				if _, err := file.Write(payload); err != nil {
					return SegmentMeta{}, err
				}
				offset += uint64(len(payload))
				if len(colMeta.Pages) == 0 {
					firstCodec = pageMeta.Codec
				} else if !mixedCodec && pageMeta.Codec != firstCodec {
					mixedCodec = true
				}
				colMeta.Pages = append(colMeta.Pages, pageMeta)
				colMeta.NullCount += pageMeta.NullCount
				colMeta.Int32 = mergeInt32Stats(colMeta.Int32, pageMeta.Int32)
				colMeta.Int64 = mergeInt64Stats(colMeta.Int64, pageMeta.Int64)
				colMeta.Text = mergeTextStats(colMeta.Text, pageMeta.Text)
			}
			rowStart += batch.Len
		}
		if !mixedCodec && firstCodec != CodecInvalid {
			colMeta.Codec = firstCodec
		}
		colMeta.AllValid = colMeta.NullCount == 0
		colMeta.AllNull = colMeta.NullCount == colMeta.Rows
		meta.Columns = append(meta.Columns, colMeta)
	}

	footer, err := json.Marshal(meta)
	if err != nil {
		return SegmentMeta{}, err
	}
	if len(footer) > int(^uint32(0)) {
		return SegmentMeta{}, fmt.Errorf("segment footer too large")
	}
	if _, err := file.Write(footer); err != nil {
		return SegmentMeta{}, err
	}
	var footerLen [4]byte
	binary.LittleEndian.PutUint32(footerLen[:], uint32(len(footer)))
	if _, err := file.Write(footerLen[:]); err != nil {
		return SegmentMeta{}, err
	}
	if _, err := file.WriteString(segmentMagic); err != nil {
		return SegmentMeta{}, err
	}
	if err := file.Sync(); err != nil {
		return SegmentMeta{}, err
	}
	if err := file.Close(); err != nil {
		return SegmentMeta{}, err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return SegmentMeta{}, err
	}
	committed = true
	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		return SegmentMeta{}, err
	}
	return meta, nil
}

func readSegmentFooter(path string) (SegmentMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return SegmentMeta{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return SegmentMeta{}, err
	}
	minSize := int64(len(segmentMagic)*2 + 4)
	if info.Size() < minSize {
		return SegmentMeta{}, fmt.Errorf("segment %q is too small", path)
	}
	header := make([]byte, len(segmentMagic))
	if _, err := file.ReadAt(header, 0); err != nil {
		return SegmentMeta{}, err
	}
	if string(header) != segmentMagic {
		return SegmentMeta{}, fmt.Errorf("segment %q has invalid header", path)
	}

	tail := make([]byte, len(segmentMagic)+4)
	if _, err := file.ReadAt(tail, info.Size()-int64(len(tail))); err != nil {
		return SegmentMeta{}, err
	}
	if string(tail[4:]) != segmentMagic {
		return SegmentMeta{}, fmt.Errorf("segment %q has invalid footer magic", path)
	}
	footerLen := binary.LittleEndian.Uint32(tail[:4])
	footerStart := info.Size() - int64(len(tail)) - int64(footerLen)
	if footerStart < int64(len(segmentMagic)) {
		return SegmentMeta{}, fmt.Errorf("segment %q has invalid footer length", path)
	}
	footer := make([]byte, footerLen)
	if _, err := file.ReadAt(footer, footerStart); err != nil {
		return SegmentMeta{}, err
	}
	var meta SegmentMeta
	if err := json.Unmarshal(footer, &meta); err != nil {
		return SegmentMeta{}, err
	}
	return meta, nil
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

