// Interactive shell and one-shot exec frontend for the embedded DripSQL engine.
// Output format is selectable and per-statement timing is always shown.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/kylegrahammatzen/dripsql"
)

func main() {
	dbPath := flag.String("db", "", "database directory (required)")
	execText := flag.String("exec", "", "run a single SQL string and exit")
	format := flag.String("format", "table", "output format, one of table or tsv or json")
	flag.Parse()

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "dripsql: -db is required")
		os.Exit(2)
	}
	mode, err := parseFormat(*format)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dripsql:", err)
		os.Exit(2)
	}

	db, err := dripsql.Open(*dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx := context.Background()
	if *execText != "" {
		remaining := *execText
		for {
			idx := topLevelSemicolon(remaining)
			if idx < 0 {
				if err := runOne(ctx, db, remaining, mode, true, os.Stdout); err != nil {
					fmt.Fprintln(os.Stderr, "error:", err)
					os.Exit(1)
				}
				return
			}
			if err := runOne(ctx, db, remaining[:idx+1], mode, true, os.Stdout); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
				os.Exit(1)
			}
			remaining = remaining[idx+1:]
		}
	}
	if err := runShell(ctx, db, mode); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
