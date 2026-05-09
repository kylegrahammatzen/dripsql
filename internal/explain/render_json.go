package explain

import (
	"encoding/json"
	"io"
)

// RenderJSON writes the report as indented JSON. JSON tags on the report
// types (see report.go and plan.go) define the wire shape.
func RenderJSON(w io.Writer, r *QueryReport) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}
