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
}

func (it *SegmentScanIterator) ForEach(visit func(types.Batch, types.SelectionMask) error) error {
	ctx := it.Context
	if ctx == nil {
		ctx = context.Background()
	}
	sel := &it.sel
	for segmentIndex := range it.Segments {
		segment := &it.Segments[segmentIndex]
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(segment.Pages) != 0 || segment.Path == "" {
			it.Stats.ObserveSegment(true)
			for _, page := range segment.Pages {
				if err := ctx.Err(); err != nil {
					return err
				}
				matched := page.Batch.Len
				if it.Predicate == nil {
					sel.Resize(page.Batch.Len)
					matched = sel.FillAll()
				} else {
					var err error
					matched, err = it.Predicate.Eval(page.Batch, sel)
					if err != nil {
						return err
					}
				}
				it.Stats.ObservePage(page.Batch.Len, page.PayloadBytes, matched, true)
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
			it.Stats.ObserveSegment(false)
			if err := it.observeSkippedSegment(segment); err != nil {
				return err
			}
			continue
		}
		it.Stats.ObserveSegment(true)
		plan, err := it.segmentReadPlan(segmentIndex, segment)
		if err != nil {
			return err
		}
		if it.usesLateMaterialization() {
			if err := it.forEachLateMaterialized(ctx, segmentIndex, segment, plan, prunePlan, sel, visit); err != nil {
				return err
			}
			continue
		}
		for pageIndex, info := range plan.PageInfos {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !prunePlan.PageCandidate(segment.Meta, pageIndex) {
				it.Stats.ObservePage(int(info.Rows), 0, 0, false)
				continue
			}
			batch, payloadBytes, err := plan.ReadBatchReused(pageIndex)
			if err != nil {
				return err
			}
			matched := batch.Len
			if it.Predicate == nil {
				sel.Resize(batch.Len)
				matched = sel.FillAll()
			} else {
				var err error
				matched, err = it.Predicate.Eval(batch, sel)
				if err != nil {
					return err
				}
			}
			it.Stats.ObservePage(batch.Len, payloadBytes, matched, true)
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
	for pageIndex, info := range plan.PageInfos {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !prunePlan.PageCandidate(segment.Meta, pageIndex) {
			it.Stats.ObservePage(int(info.Rows), 0, 0, false)
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
		it.Stats.ObservePage(batch.Len, payloadBytes, matched, true)
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

func (it *SegmentScanIterator) segmentReadPlan(segmentIndex int, segment *ScanSegment) (*SegmentReadPlan, error) {
	if len(it.readPlans) != len(it.Segments) {
		it.readPlans = make([]*SegmentReadPlan, len(it.Segments))
	}
	if plan := it.readPlans[segmentIndex]; plan != nil {
		return plan, nil
	}
	plan, err := newSegmentReadPlan(segment.Path, &segment.Meta, it.segmentReadColumns(), segment.Size, segment.PageInfos, it.fileCache)
	if err != nil {
		return nil, err
	}
	plan.Encoded = it.encodedReadColumns()
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
	plan, err := newSegmentReadPlan(segment.Path, &segment.Meta, it.Predicate.RequiredColumns(), segment.Size, segment.PageInfos, it.fileCache)
	if err != nil {
		return nil, err
	}
	plan.Encoded = it.encodedPredicateColumns()
	it.predPlans[segmentIndex] = plan
	return plan, nil
}

func (it *SegmentScanIterator) segmentOutputReadPlan(segmentIndex int, segment *ScanSegment) (*SegmentReadPlan, error) {
	missing := it.missingOutputColumns()
	if len(missing) == 0 {
		return nil, nil
	}
	if len(it.outputPlans) != len(it.Segments) {
		it.outputPlans = make([]*SegmentReadPlan, len(it.Segments))
	}
	if plan := it.outputPlans[segmentIndex]; plan != nil {
		return plan, nil
	}
	plan, err := newSegmentReadPlan(segment.Path, &segment.Meta, missing, segment.Size, segment.PageInfos, it.fileCache)
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

func (it *SegmentScanIterator) segmentReadColumns() []string {
	if it.OutputColumns == nil {
		return nil
	}
	columns := make([]string, 0, len(it.OutputColumns)+4)
	columns = append(columns, it.OutputColumns...)
	if it.Predicate != nil {
		columns = append(columns, it.Predicate.RequiredColumns()...)
	}
	return columns
}

func (it *SegmentScanIterator) usesLateMaterialization() bool {
	return it.Predicate != nil && it.OutputColumns != nil
}

func (it *SegmentScanIterator) missingOutputColumns() []string {
	if len(it.OutputColumns) == 0 {
		return nil
	}
	predicate := make(map[string]struct{})
	if it.Predicate != nil {
		for _, column := range it.Predicate.RequiredColumns() {
			predicate[column] = struct{}{}
		}
	}
	missing := make([]string, 0, len(it.OutputColumns))
	for _, column := range it.OutputColumns {
		if _, ok := predicate[column]; ok {
			continue
		}
		missing = append(missing, column)
	}
	return missing
}

func (it *SegmentScanIterator) encodedPredicateColumns() map[string]struct{} {
	if it.Predicate == nil || it.OutputColumns == nil {
		return nil
	}
	output := make(map[string]struct{}, len(it.OutputColumns))
	for _, column := range it.OutputColumns {
		output[column] = struct{}{}
	}
	encoded := make(map[string]struct{})
	for _, column := range it.Predicate.RequiredColumns() {
		if _, isOutput := output[column]; isOutput {
			if _, wantsEncoded := it.EncodedOutputColumns[column]; !wantsEncoded {
				continue
			}
		}
		encoded[column] = struct{}{}
	}
	if len(encoded) == 0 {
		return nil
	}
	return encoded
}

func (it *SegmentScanIterator) encodedReadColumns() map[string]struct{} {
	encoded := it.encodedPredicateColumns()
	if len(it.EncodedOutputColumns) == 0 {
		return encoded
	}
	if encoded == nil {
		encoded = make(map[string]struct{}, len(it.EncodedOutputColumns))
	}
	for column := range it.EncodedOutputColumns {
		encoded[column] = struct{}{}
	}
	return encoded
}
