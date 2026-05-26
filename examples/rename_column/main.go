// Demonstrates ALTER TABLE RENAME COLUMN as a metadata-only operation.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/kylegrahammatzen/dripsql"
)

func main() {
	dbPath := flag.String("db", "", "fresh database directory (defaults to a temp dir; persisted dbs may fail re-runs)")
	flag.Parse()
	if *dbPath == "" {
		d, err := os.MkdirTemp("", "dripsql-rename-column-*")
		if err != nil {
			log.Fatal(err)
		}
		*dbPath = d
		fmt.Println("using temp db:", d)
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

	if _, err := db.Query(ctx, "SELECT name FROM users"); err == nil {
		log.Fatal("expected error querying old column name")
	} else {
		fmt.Printf("old name rejected: %v\n", err)
	}
}
