package main

import (
	"context"
	"flag"
	"fmt"
	"log"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	dbPath := flag.String("db", "", "directory to open as the database (required)")
	flag.Parse()
	if flag.NArg() != 1 {
		return fmt.Errorf("usage: examples [-db <path>] <embed|read_only|transactions>")
	}
	if *dbPath == "" {
		return fmt.Errorf("missing -db <path>")
	}
	switch flag.Arg(0) {
	case "embed":
		return runEmbed(ctx, *dbPath)
	case "read_only":
		return runReadOnly(ctx, *dbPath)
	case "transactions":
		return runTransactions(ctx, *dbPath)
	}
	return fmt.Errorf("unknown example %q", flag.Arg(0))
}
