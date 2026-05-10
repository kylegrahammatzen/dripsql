package storage

import (
	"context"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type ScanPage struct {
	Batch        types.Batch
	PayloadBytes int
}

type ScanSegment struct {
	Path      string
	Size      int64
	Meta      SegmentMeta
	Pages     []ScanPage
	PageInfos []SegmentPageInfo
}

type SegmentScanIterator struct {
	Context              context.Context
	Segments             []ScanSegment
	Predicate            PredicateEvaluator
	Prune                Predicate
	OutputColumns        []string
	EncodedOutputColumns map[string]struct{}
	Stats                *ExecStats
	fileCache            *segmentFileCache
	readPlans            []*SegmentReadPlan
	predPlans            []*SegmentReadPlan
	outputPlans          []*SegmentReadPlan
	prunePlans           []PredicatePrunePlan
	pruneReady           []bool
	sel                  types.SelectionMask

	// Derived metadata cached on first ForEach so the per-page hot paths
	// don't reallocate column slices/maps each iteration.
	metaReady          bool
	late               bool
	predicateColumns   []string
	missingOutputCols  []string
	readColumns        []string
	encodedReadCols    map[string]struct{}
	encodedPredCols    map[string]struct{}
}

func (it *SegmentScanIterator) ForEach(visit func(types.Batch, types.SelectionMask) error) error {
	it.ensureMeta()
	pred := it.Predicate
	stats := it.Stats
	ctx := it.Context
	sel := &it.sel
	late := it.late

	for segmentIndex := range it.Segments {
		segment := &it.Segments[segmentIndex]
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if len(segment.Pages) != 0 || segment.Path == "" {
			stats.ObserveSegment(true)
			for _, page := range segment.Pages {
				if ctx != nil {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				matched := page.Batch.Len
				if pred == nil {
					sel.Resize(page.Batch.Len)
					matched = sel.FillAll()
				} else {
					var err error
					matched, err = pred.Eval(page.Batch, sel)
					if err != nil {
						return err
					}
				}
				stats.ObservePage(page.Batch.Len, page.PayloadBytes, matched, true)
				if matched == 0 {
					continue
				}
				if err := visit(page.Batch, *sel); err != nil {
					return err
				}
			}
			continue
		}
		prunePlan := it.segmentPrunePlan(segmentIndex, segment.Meta)
		if !prunePlan.SegmentCandidate(segment.Meta) {
			// Skip page-level accounting entirely when stats are off — the whole
			// point of segment pruning is to bail before touching pages.
			if stats != nil {
				stats.ObserveSegment(false)
				if err := it.observeSkippedSegment(segment); err != nil {
					return err
				}
			}
			continue
		}
		stats.ObserveSegment(true)
		plan, err := it.segmentReadPlan(segmentIndex, segment)
		if err != nil {
			return err
		}
		if late {
			if err := it.forEachLateMaterialized(ctx, segmentIndex, segment, plan, prunePlan, sel, visit); err != nil {
				return err
			}
			continue
		}
		for pageIndex, info := range plan.PageInfos {
			if ctx != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if !prunePlan.PageCandidate(segment.Meta, pageIndex) {
				stats.ObservePage(int(info.Rows), 0, 0, false)
				continue
			}
			batch, payloadBytes, err := plan.ReadBatchReused(pageIndex)
			if err != nil {
				return err
			}
			matched := batch.Len
			if pred == nil {
				sel.Resize(batch.Len)
				matched = sel.FillAll()
			} else {
				var err error
				matched, err = pred.Eval(batch, sel)
				if err != nil {
					return err
				}
			}
			stats.ObservePage(batch.Len, payloadBytes, matched, true)
			if matched == 0 {
				continue
			}
			if err := visit(batch, *sel); err != nil {
				return err
			}
		}
	}
	return nil
}

func (it *SegmentScanIterator) forEachLateMaterialized(ctx context.Context, segmentIndex int, segment *ScanSegment, plan *SegmentReadPlan, prunePlan PredicatePrunePlan, sel *types.SelectionMask, visit func(types.Batch, types.SelectionMask) error) error {
	predPlan, err := it.segmentPredicateReadPlan(segmentIndex, segment)
	if err != nil {
		return err
	}
	stats := it.Stats
	for pageIndex, info := range plan.PageInfos {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if !prunePlan.PageCandidate(segment.Meta, pageIndex) {
			stats.ObservePage(int(info.Rows), 0, 0, false)
			continue
		}
		batch, payloadBytes, err := predPlan.ReadBatchReused(pageIndex)
		if err != nil {
			return err
		}
		matched, err := it.Predicate.Eval(batch, sel)
		if err != nil {
			return err
		}
		if matched != 0 {
			outputBatch, outputPayload, err := it.materializeOutputColumns(segmentIndex, segment, pageIndex, *sel, batch)
			if err != nil {
				return err
			}
			payloadBytes += outputPayload
			batch = outputBatch
		}
		stats.ObservePage(batch.Len, payloadBytes, matched, true)
		if matched == 0 {
			continue
		}
		if err := visit(batch, *sel); err != nil {
			return err
		}
	}
	return nil
}

func (it *SegmentScanIterator) observeSkippedSegment(segment *ScanSegment) error {
	infos := segment.PageInfos
	if len(infos) == 0 {
		built, err := buildSegmentPageInfos(segment.Meta, nil)
		if err != nil {
			return err
		}
		infos = built
	}
	for _, info := range infos {
		it.Stats.ObservePage(int(info.Rows), 0, 0, false)
	}
	return nil
}

// ensureMeta computes the column-projection metadata derived from the
// iterator's immutable config (Predicate, OutputColumns, EncodedOutputColumns).
// All fields it produces are read-only after this call so subsequent ForEach
// invocations only pay this cost once.
func (it *SegmentScanIterator) ensureMeta() {
	if it.metaReady {
		return
	}
	if it.Predicate != nil {
		it.predicateColumns = it.Predicate.RequiredColumns()
	}
	it.late = it.Predicate != nil && it.OutputColumns != nil

	if len(it.OutputColumns) > 0 {
		var predSet map[string]struct{}
		if len(it.predicateColumns) > 0 {
			predSet = make(map[string]struct{}, len(it.predicateColumns))
			for _, column := range it.predicateColumns {
				predSet[column] = struct{}{}
			}
		}
		missing := make([]string, 0, len(it.OutputColumns))
		for _, column := range it.OutputColumns {
			if _, ok := predSet[column]; ok {
				continue
			}
			missing = append(missing, column)
		}
		it.missingOutputCols = missing
	}

	if it.OutputColumns != nil {
		read := make([]string, 0, len(it.OutputColumns)+len(it.predicateColumns))
		read = append(read, it.OutputColumns...)
		read = append(read, it.predicateColumns...)
		it.readColumns = read
	}

	if it.late {
		outputSet := make(map[string]struct{}, len(it.OutputColumns))
		for _, column := range it.OutputColumns {
			outputSet[column] = struct{}{}
		}
		encoded := make(map[string]struct{})
		for _, column := range it.predicateColumns {
			if _, isOutput := outputSet[column]; isOutput {
				if _, wantsEncoded := it.EncodedOutputColumns[column]; !wantsEncoded {
					continue
				}
			}
			encoded[column] = struct{}{}
		}
		if len(encoded) > 0 {
			it.encodedPredCols = encoded
		}
	}

	encoded := it.encodedPredCols
	if len(it.EncodedOutputColumns) != 0 {
		merged := make(map[string]struct{}, len(encoded)+len(it.EncodedOutputColumns))
		for column := range encoded {
			merged[column] = struct{}{}
		}
		for column := range it.EncodedOutputColumns {
			merged[column] = struct{}{}
		}
		encoded = merged
	}
	it.encodedReadCols = encoded

	it.metaReady = true
}

func (it *SegmentScanIterator) segmentReadPlan(segmentIndex int, segment *ScanSegment) (*SegmentReadPlan, error) {
	if len(it.readPlans) != len(it.Segments) {
		it.readPlans = make([]*SegmentReadPlan, len(it.Segments))
	}
	if plan := it.readPlans[segmentIndex]; plan != nil {
		return plan, nil
	}
	plan, err := newSegmentReadPlan(segment.Path, &segment.Meta, it.readColumns, segment.Size, segment.PageInfos, it.fileCache)
	if err != nil {
		return nil, err
	}
	plan.Encoded = it.encodedReadCols
	plan.EncodedConstants = it.Predicate == nil && len(it.EncodedOutputColumns) != 0
	it.readPlans[segmentIndex] = plan
	return plan, nil
}

func (it *SegmentScanIterator) segmentPredicateReadPlan(segmentIndex int, segment *ScanSegment) (*SegmentReadPlan, error) {
	if len(it.predPlans) != len(it.Segments) {
		it.predPlans = make([]*SegmentReadPlan, len(it.Segments))
	}
	if plan := it.predPlans[segmentIndex]; plan != nil {
		return plan, nil
	}
	plan, err := newSegmentReadPlan(segment.Path, &segment.Meta, it.predicateColumns, segment.Size, segment.PageInfos, it.fileCache)
	if err != nil {
		return nil, err
	}
	plan.Encoded = it.encodedPredCols
	it.predPlans[segmentIndex] = plan
	return plan, nil
}

func (it *SegmentScanIterator) segmentOutputReadPlan(segmentIndex int, segment *ScanSegment) (*SegmentReadPlan, error) {
	if len(it.missingOutputCols) == 0 {
		return nil, nil
	}
	if len(it.outputPlans) != len(it.Segments) {
		it.outputPlans = make([]*SegmentReadPlan, len(it.Segments))
	}
	if plan := it.outputPlans[segmentIndex]; plan != nil {
		return plan, nil
	}
	plan, err := newSegmentReadPlan(segment.Path, &segment.Meta, it.missingOutputCols, segment.Size, segment.PageInfos, it.fileCache)
	if err != nil {
		return nil, err
	}
	plan.Encoded = it.EncodedOutputColumns
	plan.EncodedConstants = len(it.EncodedOutputColumns) != 0
	it.outputPlans[segmentIndex] = plan
	return plan, nil
}

func (it *SegmentScanIterator) materializeOutputColumns(segmentIndex int, segment *ScanSegment, pageIndex int, sel types.SelectionMask, batch types.Batch) (types.Batch, int, error) {
	plan, err := it.segmentOutputReadPlan(segmentIndex, segment)
	if err != nil || plan == nil {
		return batch, 0, err
	}
	output, payloadBytes, err := plan.ReadBatchSelected(pageIndex, sel)
	if err != nil {
		return types.Batch{}, 0, err
	}
	cols := make([]types.Column, 0, len(batch.Columns)+len(output.Columns))
	cols = append(cols, batch.Columns...)
	cols = append(cols, output.Columns...)
	merged, err := types.NewBatchNoClone(cols)
	if err != nil {
		return types.Batch{}, 0, err
	}
	return merged, payloadBytes, nil
}

func (it *SegmentScanIterator) segmentPrunePlan(segmentIndex int, meta SegmentMeta) PredicatePrunePlan {
	if len(it.prunePlans) != len(it.Segments) {
		it.prunePlans = make([]PredicatePrunePlan, len(it.Segments))
		it.pruneReady = make([]bool, len(it.Segments))
	}
	if it.pruneReady[segmentIndex] {
		return it.prunePlans[segmentIndex]
	}
	plan := BindPrunePredicate(it.Prune, meta)
	it.prunePlans[segmentIndex] = plan
	it.pruneReady[segmentIndex] = true
	return plan
}
