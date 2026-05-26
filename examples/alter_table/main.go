// Walks through ALTER TABLE ADD COLUMN with a default, ALTER COLUMN TYPE widening, and DROP COLUMN.
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
	dbPath := flag.String("db", "", "fresh database directory (defaults to a temp dir; persisted dbs may fail re-runs)")
	flag.Parse()
	if *dbPath == "" {
		d, err := os.MkdirTemp("", "dripsql-alter-table-*")
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

	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS users (id int32 NOT NULL, name text NOT NULL)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(ctx, "ALTER TABLE users ADD COLUMN tag text NOT NULL DEFAULT 'unset'"); err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT id, name, tag FROM users ORDER BY id")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("after ADD COLUMN tag with default:")
	for rows.Next() {
		var id int32
		var name, tag string
		if err := rows.Scan(&id, &name, &tag); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %d %s %s\n", id, name, tag)
	}
	rows.Close()

	if _, err := db.Exec(ctx, "ALTER TABLE users ALTER COLUMN id TYPE int64"); err != nil {
		log.Fatal(err)
	}
	rows, err = db.Query(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("after ALTER COLUMN id TYPE int64 (widening, segment data cast on read):")
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %d %s\n", id, name)
	}
	rows.Close()

	if _, err := db.Exec(ctx, "ALTER TABLE users DROP COLUMN tag"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Query(ctx, "SELECT tag FROM users"); err == nil {
		log.Fatal("expected error querying dropped column")
	} else {
		fmt.Printf("after DROP COLUMN tag: %v\n", err)
	}

	cols, err := db.TableSchema("users")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("final schema:")
	for _, c := range cols {
		fmt.Printf("  %s %s nullable=%v\n", c.Name, c.Type, c.Nullable)
	}
}
