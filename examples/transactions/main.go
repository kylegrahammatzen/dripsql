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
		d, err := os.MkdirTemp("", "dripsql-transactions-*")
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

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS txn_events (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		log.Fatal(err)
	}

	err = db.Update(ctx, func(tx *dripsql.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO txn_events (id, kind) VALUES (1, 'click'), (2, 'view')"); err != nil {
			return err
		}
		var n int64
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM txn_events").Scan(&n); err != nil {
			return err
		}
		fmt.Printf("staged rows visible inside tx: %d\n", n)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
