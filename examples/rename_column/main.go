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

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS users (id int64 NOT NULL, name text NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		log.Fatal(err)
	}

	var before string
	if err := db.QueryRow(ctx, "SELECT name FROM users WHERE id = ?", int64(1)).Scan(&before); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("before rename: name = %q\n", before)

	if _, err := db.Exec(ctx, "ALTER TABLE users RENAME COLUMN name TO label"); err != nil {
		log.Fatal(err)
	}

	var after string
	if err := db.QueryRow(ctx, "SELECT label FROM users WHERE id = ?", int64(1)).Scan(&after); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("after rename:  label = %q\n", after)

	// The old column name no longer resolves, even though the segment files on disk
	// still reference it. The catalog owns the user-visible name; the scan path
	// resolves columns by stable id so the rename is metadata only.
	if _, err := db.Query(ctx, "SELECT name FROM users"); err == nil {
		log.Fatal("expected error querying old column name")
	} else {
		fmt.Printf("old name rejected: %v\n", err)
	}
}
