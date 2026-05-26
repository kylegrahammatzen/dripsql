package main

import (
	"context"
	"errors"
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

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS readonly_nums (id int64 NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO readonly_nums (id) VALUES (1), (2), (3)"); err != nil {
		log.Fatal(err)
	}

	db.SetReadOnly(true)
	if _, err := db.Exec(ctx, "INSERT INTO readonly_nums (id) VALUES (4)"); errors.Is(err, dripsql.ErrReadOnly) {
		fmt.Println("write rejected by read-only mode")
	}

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM readonly_nums").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows: %d\n", n)
}
