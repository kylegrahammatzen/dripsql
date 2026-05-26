// Writes rows, closes the database, reopens it, and confirms the rows survived.
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
	dbPath := flag.String("db", "", "database directory (defaults to a fresh temp dir)")
	flag.Parse()
	if *dbPath == "" {
		d, err := os.MkdirTemp("", "dripsql-durability-*")
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
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS durable_rows (id int64 NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO durable_rows (id) VALUES (1), (2), (3)"); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db, err = dripsql.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM durable_rows").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after reopen: %d\n", n)
}
