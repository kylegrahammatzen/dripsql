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
	dbPath := flag.String("db", "", "directory to open as the database (required)")
	flag.Parse()
	if *dbPath == "" {
		log.Fatal("missing -db <path>")
	}

	db, err := dripsql.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS events (id int64 NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO events (id) VALUES (1), (2), (3)"); err != nil {
		log.Fatal(err)
	}

	db.SetReadOnly(true)
	if _, err := db.Exec(ctx, "INSERT INTO events (id) VALUES (4)"); err != nil {
		fmt.Printf("write rejected as expected: %v\n", err)
	}

	rows, err := db.Query(ctx, "SELECT count(*) FROM events")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("read in read-only mode: %d rows\n", n)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
