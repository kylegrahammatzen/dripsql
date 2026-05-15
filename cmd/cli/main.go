// CLI. Opens a DB at -db and runs each statement read from stdin or -exec, prints results.
// SELECT and EXPLAIN dispatch to Query and render as tab-separated columns. Everything else goes to Exec.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
)

func main() {
	dbPath := flag.String("db", "", "database directory (required)")
	execText := flag.String("exec", "", "run a single SQL string and exit")
	flag.Parse()

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "dripsql: -db is required")
		os.Exit(2)
	}

	db, err := engine.Open(*dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx := context.Background()
	if *execText != "" {
		if err := runOne(ctx, db, *execText, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	if err := repl(ctx, db, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func repl(ctx context.Context, db *engine.DB, in io.Reader, out io.Writer) error {
	r := bufio.NewReader(in)
	w := bufio.NewWriter(out)
	defer w.Flush()
	interactive := isTerminal(in)
	var buf strings.Builder
	if interactive {
		fmt.Fprint(w, "dripsql> ")
		w.Flush()
	}
	for {
		line, err := r.ReadString('\n')
		if len(line) > 0 {
			buf.WriteString(line)
		}
		if err == io.EOF {
			if strings.TrimSpace(buf.String()) != "" {
				if runErr := runOne(ctx, db, buf.String(), w); runErr != nil {
					fmt.Fprintln(w, "error:", runErr)
				}
				w.Flush()
			}
			return nil
		}
		if err != nil {
			return err
		}
		if !strings.Contains(line, ";") {
			continue
		}
		stmt := buf.String()
		buf.Reset()
		if runErr := runOne(ctx, db, stmt, w); runErr != nil {
			fmt.Fprintln(w, "error:", runErr)
		}
		w.Flush()
		if interactive {
			fmt.Fprint(w, "dripsql> ")
			w.Flush()
		}
	}
}

func runOne(ctx context.Context, db *engine.DB, sqlText string, out io.Writer) error {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return nil
	}
	if isQuery(trimmed) {
		rows, err := db.Query(ctx, trimmed)
		if err != nil {
			return err
		}
		return renderRows(out, rows)
	}
	result, err := db.Exec(ctx, trimmed)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "ok (%d statement%s, %d row%s affected)\n",
		result.Statements, plural(result.Statements),
		result.RowsAffected, plural(int(result.RowsAffected)))
	return nil
}

func isQuery(sqlText string) bool {
	head := strings.ToLower(firstWord(sqlText))
	return head == "select" || head == "explain"
}

func firstWord(s string) string {
	s = strings.TrimSpace(s)
	for i, r := range s {
		if r == ' ' || r == '\t' || r == '\n' {
			return s[:i]
		}
	}
	return s
}

func renderRows(out io.Writer, rows *engine.Rows) error {
	if len(rows.Columns) == 0 {
		fmt.Fprintln(out, "(no rows)")
		return nil
	}
	fmt.Fprintln(out, strings.Join(rows.Columns, "\t"))
	for _, row := range rows.Values {
		fields := make([]string, len(row))
		for i, v := range row {
			fields[i] = formatValue(v)
		}
		fmt.Fprintln(out, strings.Join(fields, "\t"))
	}
	fmt.Fprintf(out, "(%d row%s)\n", len(rows.Values), plural(len(rows.Values)))
	return nil
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case string:
		return x
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

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func isTerminal(r io.Reader) bool {
	f, ok := r.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
