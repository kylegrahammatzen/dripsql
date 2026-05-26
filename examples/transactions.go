package main

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql"
)

func runTransactions(ctx context.Context, dbPath string) error {
	db, err := dripsql.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS txn_events (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		return err
	}

	return db.Update(ctx, func(tx *dripsql.Tx) error {
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
}
