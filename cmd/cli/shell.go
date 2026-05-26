// Interactive shell with line editing and persistent history backed by ergochat/readline.
// Multi-line statements accumulate until a top-level semicolon and meta commands start with a dot.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ergochat/readline"
	"github.com/kylegrahammatzen/dripsql"
)

func runShell(ctx context.Context, db *dripsql.DB, format formatMode) error {
	histFile := ""
	if dir, err := os.UserCacheDir(); err == nil && dir != "" {
		histDir := filepath.Join(dir, "dripsql")
		if err := os.MkdirAll(histDir, 0o755); err == nil {
			histFile = filepath.Join(histDir, "shell-history")
		}
	}
	rl, err := readline.NewFromConfig(&readline.Config{
		Prompt:          "dripsql> ",
		HistoryFile:     histFile,
		HistoryLimit:    1000,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		return fmt.Errorf("readline: %w", err)
	}
	defer rl.Close()

	out := rl.Stdout()
	fmt.Fprintln(out, "DripSQL shell, type .help for meta commands and .quit to exit.")

	timing := true
	var buf strings.Builder
	for {
		if buf.Len() == 0 {
			rl.SetPrompt("dripsql> ")
		} else {
			rl.SetPrompt("   ...> ")
		}
		line, err := rl.ReadLine()
		if err == readline.ErrInterrupt {
			buf.Reset()
			continue
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		trimmed := strings.TrimSpace(line)
		if buf.Len() == 0 && strings.HasPrefix(trimmed, ".") {
			quit, ferr := runMeta(db, trimmed, &format, &timing, out)
			if ferr != nil {
				fmt.Fprintln(out, "error:", ferr)
			}
			if quit {
				return nil
			}
			continue
		}

		buf.WriteString(line)
		buf.WriteByte('\n')
		for {
			pending := buf.String()
			idx := topLevelSemicolon(pending)
			if idx < 0 {
				break
			}
			stmt := pending[:idx+1]
			buf.Reset()
			buf.WriteString(pending[idx+1:])
			if err := runOne(ctx, db, stmt, format, timing, out); err != nil {
				fmt.Fprintln(out, "error:", err)
			}
		}
	}
}

func runOne(ctx context.Context, db *dripsql.DB, sqlText string, format formatMode, timing bool, out io.Writer) error {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return nil
	}
	fields := strings.Fields(strings.ToLower(trimmed))
	head := fields[0]
	if head == "select" || head == "explain" || head == "with" {
		start := time.Now()
		rows, err := db.Query(ctx, trimmed)
		if err != nil {
			return err
		}
		defer rows.Close()
		cols := rows.Columns()
		values, err := rows.All()
		if err != nil {
			return err
		}
		elapsed := time.Since(start)
		if err := renderRows(out, format, cols, values); err != nil {
			return err
		}
		fmt.Fprintf(out, "(%d row%s)\n", len(values), plural(len(values)))
		if timing {
			fmt.Fprintf(out, "time %s\n", formatDuration(elapsed))
		}
		return nil
	}
	start := time.Now()
	result, err := db.Exec(ctx, trimmed)
	elapsed := time.Since(start)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "ok (%d statement%s, %d row%s affected)\n",
		result.Statements, plural(result.Statements),
		result.RowsAffected, plural(int(result.RowsAffected)))
	if timing {
		fmt.Fprintf(out, "time %s\n", formatDuration(elapsed))
	}
	return nil
}

func runMeta(db *dripsql.DB, line string, format *formatMode, timing *bool, out io.Writer) (bool, error) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return false, nil
	}
	switch parts[0] {
	case ".quit", ".exit":
		return true, nil
	case ".help":
		fmt.Fprintln(out, ".help                show this message")
		fmt.Fprintln(out, ".quit                exit the shell")
		fmt.Fprintln(out, ".tables              list catalog tables")
		fmt.Fprintln(out, ".schema NAME         show columns for one table")
		fmt.Fprintln(out, ".mode table|tsv|json switch the output format")
		fmt.Fprintln(out, ".timer on|off        toggle the per-statement time footer")
		return false, nil
	case ".tables":
		for _, name := range db.Tables() {
			fmt.Fprintln(out, name)
		}
		return false, nil
	case ".schema":
		if len(parts) < 2 {
			return false, fmt.Errorf(".schema needs a table name")
		}
		cols, err := db.TableSchema(parts[1])
		if err != nil {
			return false, err
		}
		rows := make([][]any, len(cols))
		for i, c := range cols {
			nullable := "NOT NULL"
			if c.Nullable {
				nullable = "NULL"
			}
			rows[i] = []any{c.Name, c.Type, nullable}
		}
		return false, renderRows(out, formatTable, []string{"column", "type", "nullable"}, rows)
	case ".mode":
		if len(parts) < 2 {
			fmt.Fprintln(out, "mode:", *format)
			return false, nil
		}
		mode, err := parseFormat(parts[1])
		if err != nil {
			return false, err
		}
		*format = mode
		return false, nil
	case ".timer":
		if len(parts) < 2 {
			state := "off"
			if *timing {
				state = "on"
			}
			fmt.Fprintln(out, "timer:", state)
			return false, nil
		}
		switch strings.ToLower(parts[1]) {
		case "on":
			*timing = true
		case "off":
			*timing = false
		default:
			return false, fmt.Errorf("timer wants on or off")
		}
		return false, nil
	}
	return false, fmt.Errorf("unknown meta command %q, try .help", parts[0])
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func topLevelSemicolon(s string) int {
	inStr := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' {
			if inStr && i+1 < len(s) && s[i+1] == '\'' {
				i++
				continue
			}
			inStr = !inStr
			continue
		}
		if c == ';' && !inStr {
			return i
		}
	}
	return -1
}
