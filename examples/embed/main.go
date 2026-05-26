// Minimal embed example showing Open, Exec, and the streaming Query cursor.
// Build and run with `go run ./examples/embed -db /tmp/embed_demo`.
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
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS events (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO events (id, kind) VALUES (1, 'click'), (2, 'view'), (3, 'click')"); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(ctx, "SELECT count(*) FROM events WHERE kind = 'click'")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	vals, err := rows.All()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("click count: %v\n", vals[0][0])
}
