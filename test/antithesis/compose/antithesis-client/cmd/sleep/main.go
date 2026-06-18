/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
