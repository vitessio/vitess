package main

import (
	"log"

	"vitess.io/vitess/go/cmd/rulesctl/cmd"
)

func main() {
	rootCmd := cmd.Main()
	if err := rootCmd.Execute(); err != nil {
		log.Printf("%v", err)
	}
}
