// Read-only example showing SetReadOnly blocking writes while reads still work.
// Build and run with `go run ./examples/readonly -db /tmp/readonly_demo`.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/kylegrahammatzen/dripsql"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	dbPath := flag.String("db", "", "directory to open as the database (required)")
	flag.Parse()
	if *dbPath == "" {
		return fmt.Errorf("missing -db <path>")
	}

	db, err := dripsql.Open(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS events (id int64 NOT NULL)"); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, "INSERT INTO events (id) VALUES (1), (2), (3)"); err != nil {
		return err
	}

	db.SetReadOnly(true)
	if _, err := db.Exec(ctx, "INSERT INTO events (id) VALUES (4)"); err != nil {
		fmt.Printf("write rejected as expected: %v\n", err)
	}

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&n); err != nil {
		return err
	}
	fmt.Printf("read in read-only mode: %d rows\n", n)
	return nil
}
