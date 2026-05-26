package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql"
)

func runReadOnly(ctx context.Context, dbPath string) error {
	db, err := dripsql.Open(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS readonly_nums (id int64 NOT NULL)"); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, "INSERT INTO readonly_nums (id) VALUES (1), (2), (3)"); err != nil {
		return err
	}

	db.SetReadOnly(true)
	if _, err := db.Exec(ctx, "INSERT INTO readonly_nums (id) VALUES (4)"); errors.Is(err, dripsql.ErrReadOnly) {
		fmt.Println("write rejected by read-only mode")
	}

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM readonly_nums").Scan(&n); err != nil {
		return err
	}
	fmt.Printf("rows: %d\n", n)
	return nil
}
