package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/random"
)

func main() {
	minSeconds := envInt("SLEEP_MIN", 15)
	maxSeconds := envInt("SLEEP_MAX", 300)

	seconds := minSeconds + int(random.GetRandom()%uint64(maxSeconds-minSeconds+1))
	fmt.Printf("sleeping for %d seconds (range %d-%d)\n", seconds, minSeconds, maxSeconds)
	time.Sleep(time.Duration(seconds) * time.Second)
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}
