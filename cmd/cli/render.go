// Output rendering for the three supported formats.
// table uses text/tabwriter for column alignment and tsv or json emit machine-friendly forms.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

type formatMode int

const (
	formatTable formatMode = iota
	formatTSV
	formatJSON
)

func parseFormat(s string) (formatMode, error) {
	switch strings.ToLower(s) {
	case "table":
		return formatTable, nil
	case "tsv":
		return formatTSV, nil
	case "json":
		return formatJSON, nil
	}
	return formatTable, fmt.Errorf("unknown format %q, want one of table tsv json", s)
}

func (m formatMode) String() string {
	switch m {
	case formatTSV:
		return "tsv"
	case formatJSON:
		return "json"
	}
	return "table"
}

func renderRows(out io.Writer, mode formatMode, cols []string, values [][]any) error {
	if len(cols) == 0 {
		fmt.Fprintln(out, "(no rows)")
		return nil
	}
	switch mode {
	case formatTSV:
		if _, err := fmt.Fprintln(out, strings.Join(cols, "\t")); err != nil {
			return err
		}
		for _, row := range values {
			fields := make([]string, len(row))
			for i, v := range row {
				fields[i] = formatValue(v)
			}
			if _, err := fmt.Fprintln(out, strings.Join(fields, "\t")); err != nil {
				return err
			}
		}
		return nil
	case formatJSON:
		objs := make([]map[string]any, len(values))
		for i, row := range values {
			obj := make(map[string]any, len(cols))
			for j, name := range cols {
				if j >= len(row) {
					continue
				}
				if b, ok := row[j].([]byte); ok {
					obj[name] = string(b)
				} else {
					obj[name] = row[j]
				}
			}
			objs[i] = obj
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(objs)
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(cols, "\t"))
	dashes := make([]string, len(cols))
	for i, c := range cols {
		dashes[i] = strings.Repeat("-", len(c))
	}
	fmt.Fprintln(tw, strings.Join(dashes, "\t"))
	for _, row := range values {
		fields := make([]string, len(cols))
		for i := range cols {
			if i < len(row) {
				fields[i] = formatValue(row[i])
			}
		}
		fmt.Fprintln(tw, strings.Join(fields, "\t"))
	}
	return tw.Flush()
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case int64:
		return fmt.Sprintf("%d", x)
	case float64:
		return fmt.Sprintf("%g", x)
	case bool:
		if x {
			return "true"
		}
		return "false"
	}
	return fmt.Sprintf("%v", v)
}

func formatDuration(d time.Duration) string {
	ms := float64(d.Microseconds()) / 1000.0
	switch {
	case ms < 1:
		return fmt.Sprintf("%.0f us", ms*1000)
	case ms < 1000:
		return fmt.Sprintf("%.2f ms", ms)
	default:
		return fmt.Sprintf("%.3f s", ms/1000)
	}
}
