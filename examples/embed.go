package main

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql"
)

func runEmbed(ctx context.Context, dbPath string) error {
	db, err := dripsql.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS events (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, "INSERT INTO events (id, kind) VALUES (1, 'click'), (2, 'view'), (3, 'click')"); err != nil {
		return err
	}

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM events WHERE kind = ?", "click").Scan(&n); err != nil {
		return err
	}
	fmt.Printf("click count: %d\n", n)
	return nil
}
