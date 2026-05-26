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

	ctx := context.Background()
	db, err := dripsql.Open(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS explain_users (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO explain_users (id, kind) VALUES (1, 'a'), (2, 'b'), (3, 'a')"); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(ctx, "EXPLAIN SELECT kind, count(*) FROM explain_users WHERE id > ? GROUP BY kind", int64(1))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			log.Fatal(err)
		}
		fmt.Println(line)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
