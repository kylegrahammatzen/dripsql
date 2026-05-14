// Command cli is the DripSQL v3 admin/shell entrypoint.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
)

const version = "0.0.0-dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && args[0] == "version" {
		fmt.Fprintln(stdout, version)
		return 0
	}
	if len(args) == 3 && args[0] == "exec" {
		return runExec(args[1], args[2], stdout, stderr)
	}
	if len(args) == 3 && args[0] == "query" {
		return runQuery(args[1], args[2], stdout, stderr)
	}
	printUsage(stderr)
	return 2
}

func runExec(path, sql string, stdout, stderr io.Writer) int {
	db, err := engine.Open(context.Background(), path)
	if err != nil {
		printError(stderr, err)
		return 1
	}
	defer db.Close()

	result, err := db.Exec(context.Background(), sql)
	if err != nil {
		printError(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "OK statements=%d rows_affected=%d\n", result.Statements, result.RowsAffected)
	return 0
}

func runQuery(path, sql string, stdout, stderr io.Writer) int {
	db, err := engine.Open(context.Background(), path)
	if err != nil {
		printError(stderr, err)
		return 1
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), sql)
	if err != nil {
		printError(stderr, err)
		return 1
	}
	printRows(stdout, rows)
	return 0
}

func printRows(w io.Writer, rows *engine.Rows) {
	if rows == nil {
		return
	}
	fmt.Fprintln(w, strings.Join(rows.Columns, "\t"))
	for _, row := range rows.Values {
		cells := make([]string, 0, len(row))
		for _, value := range row {
			cells = append(cells, fmt.Sprint(value))
		}
		fmt.Fprintln(w, strings.Join(cells, "\t"))
	}
}

func printError(w io.Writer, err error) {
	fmt.Fprintf(w, "dripsql: %v\n", err)
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "DripSQL experimental v3 engine")
	fmt.Fprintln(w, "usage: dripsql version")
	fmt.Fprintln(w, "       dripsql exec <db-path> <sql>")
	fmt.Fprintln(w, "       dripsql query <db-path> <sql>")
}
