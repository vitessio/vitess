package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	seconds := 5
	if v := os.Getenv("SLEEP"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			seconds = parsed
		}
	}

	fmt.Printf("sleeping for %d seconds\n", seconds)
	time.Sleep(time.Duration(seconds) * time.Second)
}
