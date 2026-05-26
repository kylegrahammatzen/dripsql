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

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS group_events (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO group_events (id, kind) VALUES (1, 'click'), (2, 'view'), (3, 'click'), (4, 'purchase'), (5, 'view')"); err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query(ctx, "SELECT kind, count(*) FROM group_events GROUP BY kind ORDER BY kind")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		var count int64
		if err := rows.Scan(&kind, &count); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %d\n", kind, count)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
