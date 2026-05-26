// Reads a historical snapshot using QueryAt and the SQL `AS OF` clause.
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
		d, err := os.MkdirTemp("", "dripsql-time-travel-*")
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

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS events (id int64 NOT NULL, kind text NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO events (id, kind) VALUES (1, 'click'), (2, 'view')"); err != nil {
		log.Fatal(err)
	}
	earlyTs := db.LastCommitTs()
	if _, err := db.Exec(ctx, "INSERT INTO events (id, kind) VALUES (3, 'purchase'), (4, 'click')"); err != nil {
		log.Fatal(err)
	}

	var nowCount int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&nowCount); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows now: %d\n", nowCount)

	earlyRows, err := db.QueryAt(ctx, "SELECT count(*) FROM events", earlyTs)
	if err != nil {
		log.Fatal(err)
	}
	var earlyAPI int64
	if earlyRows.Next() {
		if err := earlyRows.Scan(&earlyAPI); err != nil {
			log.Fatal(err)
		}
	}
	earlyRows.Close()
	fmt.Printf("rows AS OF %d (via QueryAt):   %d\n", earlyTs, earlyAPI)

	var earlySQL int64
	sqlAsOf := fmt.Sprintf("SELECT count(*) FROM events AS OF %d", earlyTs)
	if err := db.QueryRow(ctx, sqlAsOf).Scan(&earlySQL); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows AS OF %d (via SQL AS OF): %d\n", earlyTs, earlySQL)
}
