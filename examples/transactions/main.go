// Transactions example showing BeginTx with staged Exec and Query before Commit.
// Build and run with `go run ./examples/transactions -db /tmp/txn_demo`.
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

	tx, err := db.BeginTx(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(ctx, "INSERT INTO events (id, kind) VALUES (1, 'click')"); err != nil {
		log.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO events (id, kind) VALUES (2, 'view')"); err != nil {
		log.Fatal(err)
	}

	rows, err := tx.Query(ctx, "SELECT count(*) FROM events")
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("staged rows visible inside tx: %d\n", n)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	rows.Close()

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("transaction committed")
}
