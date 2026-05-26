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

	ctx := context.Background()
	db, err := dripsql.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS events (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO events (id, kind) VALUES (1, 'click'), (2, 'view'), (3, 'click')"); err != nil {
		log.Fatal(err)
	}

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM events WHERE kind = ?", "click").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("click count: %d\n", n)
}
