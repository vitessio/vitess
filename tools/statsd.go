/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// statsd is a simple server for hosting test.go remote stats.
package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var mu sync.Mutex

const statsFileName = "stats.json"

func main() {
	http.HandleFunc("/travis/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			test := r.FormValue("test")
			result := r.FormValue("result")

			if test == "" || result == "" {
				return
			}

			switch result {
			case "pass":
				duration := r.FormValue("duration")
				if duration == "" {
					return
				}
				dur, err := time.ParseDuration(duration)
				if err != nil {
					return
				}
				testPassed(test, dur)
			case "fail":
				testFailed(test)
			case "flake":
				try := r.FormValue("try")
				if try == "" {
					return
				}
				i, err := strconv.ParseInt(try, 10, 64)
				if err != nil {
					return
				}
				testFlaked(test, int(i))
			}

			return
		}

		http.ServeFile(w, r, statsFileName)
	})

	http.ListenAndServe(":15123", nil)
}

type Stats struct {
	TestStats map[string]TestStats
}

type TestStats struct {
	Pass, Fail, Flake int
	PassTime          time.Duration

	name string
}

func testPassed(name string, passTime time.Duration) {
	updateTestStats(name, func(ts *TestStats) {
		totalTime := int64(ts.PassTime)*int64(ts.Pass) + int64(passTime)
		ts.Pass++
		ts.PassTime = time.Duration(totalTime / int64(ts.Pass))
	})
}

func testFailed(name string) {
	updateTestStats(name, func(ts *TestStats) {
		ts.Fail++
	})
}

func testFlaked(name string, try int) {
	updateTestStats(name, func(ts *TestStats) {
		ts.Flake += try - 1
	})
}

func updateTestStats(name string, update func(*TestStats)) {
	var stats Stats

	mu.Lock()
	defer mu.Unlock()

	data, err := ioutil.ReadFile(statsFileName)
	if err != nil {
		log.Print("Can't read stats file, starting new one.")
	} else {
		if err := json.Unmarshal(data, &stats); err != nil {
			log.Printf("Can't parse stats file: %v", err)
			return
		}
	}

	if stats.TestStats == nil {
		stats.TestStats = make(map[string]TestStats)
	}
	ts := stats.TestStats[name]
	update(&ts)
	stats.TestStats[name] = ts

	data, err = json.MarshalIndent(stats, "", "\t")
	if err != nil {
		log.Printf("Can't encode stats file: %v", err)
		return
	}
	if err := ioutil.WriteFile(statsFileName, data, 0644); err != nil {
		log.Printf("Can't write stats file: %v", err)
	}
}
