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

package logutil

import (
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	keepLogs          = flag.Duration("keep_logs", 0*time.Second, "keep logs for this long (zero to keep forever)")
	keepLatestNLogs   = flag.Int("keep_latest_n_logs", 0, "keep the latest N log files per error level (zero to keep all)")
	purgeLogsInterval = flag.Duration("purge_logs_interval", 1*time.Hour, "how often try to remove old logs")
)

// parse parses a file name (as used by glog) and returns its process
// name and timestamp.
func parseTimestamp(filename string) (timestamp time.Time, err error) {
	parts := strings.Split(filepath.Base(filename), ".")
	if len(parts) < 6 {
		return time.Time{}, fmt.Errorf("malformed logfile name: %v", filename)
	}
	return time.ParseInLocation("20060102-150405", parts[len(parts)-2], time.Now().Location())

}

var levels = []string{"INFO", "ERROR", "WARNING", "FATAL"}

// purgeLogsByTimestamp removes log files for program for dir, if their age
// relative to now is greater than keep.
func purgeLogsByTimestamp(now time.Time, dir, program string, keep time.Duration) {
	current := getCurrentLogMap(dir, program)
	files, err := filepath.Glob(path.Join(dir, fmt.Sprintf("%s.*", program)))
	if err != nil {
		return
	}
	for _, file := range files {
		if current[file] {
			continue
		}
		created, err := parseTimestamp(file)
		if err != nil {
			continue
		}
		if now.Sub(created) > keep {
			os.Remove(file)
		}
	}
}

// purgeLogsByIndex removes all except the latest keepLogCount log files for
// program in dir
func purgeLogsByIndex(dir, program string, keepLogCount int) {
	current := getCurrentLogMap(dir, program)
	for _, level := range levels {
		files, err := filepath.Glob(path.Join(dir, fmt.Sprintf("%s.*.%s.*", program, level)))
		if err != nil {
			return
		}
		sort.Sort(byTimestamp(files))
		for fileIdx, file := range files {
			if current[file] {
				continue
			}
			if fileIdx >= keepLogCount {
				os.Remove(file)
			}
		}
	}
}

// getCurrentLogMap returns a map of current log files (current meaning
// currently being written to)
func getCurrentLogMap(dir, program string) map[string]bool {
	current := make(map[string]bool)
	for _, level := range levels {
		c, err := os.Readlink(path.Join(dir, fmt.Sprintf("%s.%s", program, level)))
		if err != nil {
			continue
		}
		current[c] = true
	}
	return current
}

// byTimestamp is a sort.Interface for sorting a []string of file paths by
// their timestamp
type byTimestamp []string

func (s byTimestamp) Len() int {
	return len(s)
}
func (s byTimestamp) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byTimestamp) Less(i, j int) bool {
	createdI, errI := parseTimestamp(s[i])
	createdJ, errJ := parseTimestamp(s[j])
	if errI != nil && errJ != nil {
		return false
	} else if errI != nil {
		return false // send parseTimestamp errors to bottom of list
	} else if errJ != nil {
		return true // send parseTimestamp errors to bottom of list
	}
	return createdJ.Before(createdI)
}

// PurgeLogs removes old log files according to `-keep_logs` and
// `-keep_latest_n_logs` flags. Current log files are never removed.
func PurgeLogs() {
	f := flag.Lookup("log_dir")
	if f == nil {
		panic("the logging module doesn't specify a log_dir flag")
	}

	if *keepLogs == 0*time.Second && *keepLatestNLogs == 0 {
		// We will never purge anything; bail early
		return
	}

	// Run purge routines every `-purge_logs_interval` seconds
	logDir := f.Value.String()
	program := filepath.Base(os.Args[0])
	timer := time.NewTimer(*purgeLogsInterval)
	for range timer.C {
		if *keepLogs != 0*time.Second {
			purgeLogsByTimestamp(time.Now(), logDir, program, *keepLogs)
		}
		if *keepLatestNLogs != 0 {
			purgeLogsByIndex(logDir, program, *keepLatestNLogs)
		}
	}
}
