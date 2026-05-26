package main

import (
	"context"
	"errors"
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
		d, err := os.MkdirTemp("", "dripsql-read-only-*")
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

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS readonly_nums (id int64 NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO readonly_nums (id) VALUES (1), (2), (3)"); err != nil {
		log.Fatal(err)
	}

	db.SetReadOnly(true)
	_, writeErr := db.Exec(ctx, "INSERT INTO readonly_nums (id) VALUES (4)")
	switch {
	case errors.Is(writeErr, dripsql.ErrReadOnly):
		fmt.Println("write rejected by read-only mode")
	case writeErr != nil:
		log.Fatalf("unexpected error: %v", writeErr)
	default:
		log.Fatal("write succeeded but read-only mode should have rejected it")
	}

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM readonly_nums").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows: %d\n", n)
}
