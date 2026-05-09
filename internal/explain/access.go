package explain

// Canonical access-decision labels. The engine populates AccessEntry.Decision
// with strings that match these constants. Storage records access decisions
// as a typed code (storage.AccessCode); the engine layer maps that code to
// one of these strings before building the AccessEntry.
const (
	AccessMinMaxPrune           = "min/max prune"
	AccessSegmentMinMaxPrune    = "segment min/max prune"
	AccessPageMinMaxPrune       = "page min/max prune"
	AccessTextSummaryPrune      = "text summary prune"
	AccessRawCountLoop          = "raw count loop"
	AccessRawIntSumLoop         = "raw int64 sum loop"
	AccessRawIntMinMaxLoop      = "raw int64 min/max loop"
	AccessTextPayloadGrouped    = "text payload grouped"
	AccessGroupedScanCount      = "counted rows per group during grouped scan"
	AccessPayloadForExpr        = "payload loaded for expression filter"
	AccessCountedAfterRowFilter = "counted after row filter"
	AccessMetadataAnswered      = "metadata answered"
	AccessMetadataNonNullCount  = "metadata non-null count"
)
