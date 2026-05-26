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
		d, err := os.MkdirTemp("", "dripsql-view-*")
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

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS view_events (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO view_events (id, kind) VALUES (1, 'click'), (2, 'view'), (3, 'click')"); err != nil {
		log.Fatal(err)
	}

	db.SetReadOnly(true)
	err = db.View(ctx, func(rtx *dripsql.ReadTx) error {
		var total int64
		if err := rtx.QueryRow(ctx, "SELECT count(*) FROM view_events").Scan(&total); err != nil {
			return err
		}
		var clicks int64
		if err := rtx.QueryRow(ctx, "SELECT count(*) FROM view_events WHERE kind = ?", "click").Scan(&clicks); err != nil {
			return err
		}
		fmt.Printf("total: %d, clicks: %d\n", total, clicks)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
