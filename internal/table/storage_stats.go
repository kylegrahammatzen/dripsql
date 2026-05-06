package table

import (
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const storageStringOffsetWidth = 4

func decorateStorageStats(batch vector.Batch, stats storage.SegmentStats) (storage.SegmentStats, error) {
	if len(stats.Columns) != len(batch.Columns) {
		return storage.SegmentStats{}, fmt.Errorf("storage stats have %d columns, batch has %d", len(stats.Columns), len(batch.Columns))
	}
	for i, col := range stats.Columns {
		batchCol := batch.Columns[i]
		if batchCol.Name != col.Name {
			return storage.SegmentStats{}, fmt.Errorf("storage stats column %d is %q, batch column is %q", i, col.Name, batchCol.Name)
		}
		plainBytes, err := storagePlainBytes(col, batchCol.Vector)
		if err != nil {
			return storage.SegmentStats{}, fmt.Errorf("column %q plain bytes: %w", col.Name, err)
		}
		filterBytes, err := checkedStorageStatInt("filter length", col.Filters.Bytes)
		if err != nil {
			return storage.SegmentStats{}, err
		}

		col.PlainBytes = plainBytes
		if col.Codec == storage.CodecDictionary {
			dictValues, idEncoding, err := dictionaryDetails(batchCol.Vector)
			if err != nil {
				return storage.SegmentStats{}, fmt.Errorf("column %q dictionary stats: %w", col.Name, err)
			}
			col.DictionaryValues = dictValues
			if idEncoding&dictionaryPackedIDFlag != 0 {
				col.DictionaryPacked = true
				col.DictionaryPackedBitWidth = idEncoding &^ dictionaryPackedIDFlag
			} else {
				col.DictionaryIDWidth = idEncoding
			}
			col.FilterPath = "dictionary"
			if col.Kind == vector.KindString {
				col.GroupPath = "dict-counts"
			}
		} else if col.Codec == storage.CodecInt64Sequence {
			col.FilterPath = "sequence"
		} else if col.Codec == storage.CodecStringPrefix {
			if err := addStorageStringFilter(batchCol.Vector, &col); err != nil {
				return storage.SegmentStats{}, fmt.Errorf("column %q filter stats: %w", col.Name, err)
			}
			if filterBytes > 0 {
				col.FilterPath = "page-bloom"
			} else if col.StringBloom != nil {
				col.FilterPath = "segment-bloom"
			}
			col.GroupPath = "bytes"
		} else if col.Codec == storage.CodecStringTemplate {
			col.GroupPath = "bytes"
		} else if col.Codec == storage.CodecPlain {
			if err := addStoragePlainFilters(batchCol.Vector, &col); err != nil {
				return storage.SegmentStats{}, fmt.Errorf("column %q filter stats: %w", col.Name, err)
			}
			if filterBytes > 0 {
				col.FilterPath = "page-bloom"
			} else if col.Int64Bloom != nil || col.StringBloom != nil {
				col.FilterPath = "segment-bloom"
			}
			if col.Kind == vector.KindString {
				col.GroupPath = "bytes"
			}
		}
		stats.Columns[i] = col
	}
	return stats, nil
}

func addStoragePlainFilters(v vector.Vector, col *storage.Column) error {
	switch values := v.(type) {
	case vector.Int64:
		bloom, err := storage.BuildInt64Bloom(values)
		if err != nil {
			return err
		}
		col.Int64Bloom = bloom
	case vector.String:
		bloom, err := storage.BuildStringBloom(values)
		if err != nil {
			return err
		}
		col.StringBloom = bloom
	case nil:
		return fmt.Errorf("nil vector")
	}
	return nil
}

func addStorageStringFilter(v vector.Vector, col *storage.Column) error {
	if v == nil {
		return fmt.Errorf("nil vector")
	}
	values, ok := v.(vector.String)
	if !ok {
		return fmt.Errorf("unsupported vector kind %s", v.Kind())
	}
	bloom, err := storage.BuildStringBloom(values)
	if err != nil {
		return err
	}
	col.StringBloom = bloom
	return nil
}

func storagePlainBytes(col storage.Column, v vector.Vector) (int, error) {
	if v == nil {
		return 0, fmt.Errorf("nil vector")
	}
	if v.Kind() != col.Kind {
		return 0, fmt.Errorf("stats kind %s does not match vector kind %s", col.Kind, v.Kind())
	}
	switch col.Kind {
	case vector.KindInt64:
		return checkedStorageStatMul("plain int64 bytes", col.Count, 8)
	case vector.KindString:
		if col.Codec == storage.CodecPlain {
			return checkedStorageStatInt("plain string bytes", col.Payload.Bytes)
		}
		return plainVectorBytes(v)
	default:
		return 0, fmt.Errorf("unsupported vector kind %s", col.Kind)
	}
}

func plainVectorBytes(v vector.Vector) (int, error) {
	switch values := v.(type) {
	case vector.Int64:
		return checkedStorageStatMul("plain int64 bytes", values.Len(), 8)
	case vector.String:
		offsetBytes, err := checkedStorageStatMul("plain string offsets", values.Len()+1, storageStringOffsetWidth)
		if err != nil {
			return 0, err
		}
		dataBytes := 0
		for row := 0; row < values.Len(); row++ {
			var err error
			dataBytes, err = checkedStorageStatAdd("plain string bytes", dataBytes, len(values.Value(row)))
			if err != nil {
				return 0, err
			}
		}
		return checkedStorageStatAdd("plain string bytes", offsetBytes, dataBytes)
	default:
		return 0, fmt.Errorf("unsupported vector kind %s", v.Kind())
	}
}

func dictionaryDetails(v vector.Vector) (values int, idEncoding int, err error) {
	switch col := v.(type) {
	case vector.Int64:
		seen := make(map[int64]struct{}, min(col.Len(), 1024))
		for _, value := range col.Values {
			seen[value] = struct{}{}
		}
		return len(seen), dictionaryIDEncoding(len(seen), col.Len()), nil
	case vector.String:
		seen := make(map[string]struct{}, min(col.Len(), 1024))
		for row := 0; row < col.Len(); row++ {
			seen[col.Value(row)] = struct{}{}
		}
		return len(seen), dictionaryIDEncoding(len(seen), col.Len()), nil
	default:
		return 0, 0, fmt.Errorf("unsupported vector kind %s", v.Kind())
	}
}

func dictionaryIDEncoding(dictCount int, rows int) int {
	fixedWidth := dictionaryIDWidth(dictCount)
	packedBitWidth := dictionaryPackedBitWidthForCount(dictCount)
	if packedBitWidth >= fixedWidth*8 {
		return fixedWidth
	}
	fixedLen, fixedErr := checkedStorageStatMul("dictionary fixed ids", rows, fixedWidth)
	packedLen, packedErr := packedDictionaryIDsLen(rows, packedBitWidth)
	if fixedErr == nil && packedErr == nil && packedLen < fixedLen {
		return dictionaryPackedIDFlag | packedBitWidth
	}
	return fixedWidth
}

func dictionaryIDWidth(dictCount int) int {
	if dictCount <= 1<<8 {
		return 1
	}
	if dictCount <= 1<<16 {
		return 2
	}
	return 4
}

const dictionaryPackedIDFlag = 1 << 7

func dictionaryPackedBitWidthForCount(dictCount int) int {
	if dictCount <= 1 {
		return 0
	}
	return bits.Len(uint(dictCount - 1))
}

func packedDictionaryIDsLen(rows int, bitWidth int) (int, error) {
	bitsLen, err := checkedStorageStatMul("dictionary packed ids", rows, bitWidth)
	if err != nil {
		return 0, err
	}
	bitsLen, err = checkedStorageStatAdd("dictionary packed ids", bitsLen, 7)
	if err != nil {
		return 0, err
	}
	return bitsLen / 8, nil
}

func checkedStorageStatInt(label string, n int64) (int, error) {
	if n < 0 {
		return 0, fmt.Errorf("negative %s %d", label, n)
	}
	if n > int64(int(^uint(0)>>1)) {
		return 0, fmt.Errorf("%s %d overflows int", label, n)
	}
	return int(n), nil
}

func checkedStorageStatAdd(label string, a int, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("negative %s component", label)
	}
	if a > int(^uint(0)>>1)-b {
		return 0, fmt.Errorf("%s overflows int", label)
	}
	return a + b, nil
}

func checkedStorageStatMul(label string, a int, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("negative %s component", label)
	}
	if a != 0 && b > int(^uint(0)>>1)/a {
		return 0, fmt.Errorf("%s overflows int", label)
	}
	return a * b, nil
}
