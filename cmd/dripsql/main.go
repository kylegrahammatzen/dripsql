package main

import (
	"fmt"
	"os"
)

const version = "0.0.0-dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}

	fmt.Println("DripSQL experimental engine")
	fmt.Println("usage: dripsql version")
}
