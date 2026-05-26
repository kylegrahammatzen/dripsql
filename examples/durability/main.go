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
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS durable_rows (id int64 NOT NULL)"); err != nil {
		db.Close()
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO durable_rows (id) VALUES (1), (2), (3)"); err != nil {
		db.Close()
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db2, err := dripsql.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()

	var n int64
	if err := db2.QueryRow(ctx, "SELECT count(*) FROM durable_rows").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after reopen: %d\n", n)
}
