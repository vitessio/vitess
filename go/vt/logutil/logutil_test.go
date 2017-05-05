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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"
)

func TestParsing(t *testing.T) {

	path := []string{
		"/tmp/something.foo/zkocc.goedel.szopa.log.INFO.20130806-151006.10530",
		"/tmp/something.foo/zkocc.goedel.szopa.test.log.ERROR.20130806-151006.10530"}

	for _, filepath := range path {
		ts, err := parseTimestamp(filepath)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		if want := time.Date(2013, 8, 6, 15, 10, 06, 0, time.Now().Location()); ts != want {
			t.Errorf("timestamp: want %v, got %v", want, ts)
		}
	}
}

func TestPurge(t *testing.T) {
	logDir := path.Join(os.TempDir(), fmt.Sprintf("%v-%v", os.Args[0], os.Getpid()))
	if err := os.MkdirAll(logDir, 0777); err != nil {
		t.Fatalf("os.MkdirAll: %v", err)
	}
	defer os.RemoveAll(logDir)

	now := time.Date(2013, 8, 6, 15, 10, 06, 0, time.Now().Location())
	files := []string{
		"zkocc.goedel.szopa.log.INFO.20130806-121006.10530",
		"zkocc.goedel.szopa.log.INFO.20130806-131006.10530",
		"zkocc.goedel.szopa.log.INFO.20130806-141006.10530",
		"zkocc.goedel.szopa.log.INFO.20130806-151006.10530",
	}

	for _, file := range files {
		if _, err := os.Create(path.Join(logDir, file)); err != nil {
			t.Fatalf("os.Create: %v", err)
		}
	}
	if err := os.Symlink(files[1], path.Join(logDir, "zkocc.INFO")); err != nil {
		t.Fatalf("os.Symlink: %v", err)
	}

	purgeLogsOnce(now, logDir, "zkocc", 30*time.Minute)

	left, err := filepath.Glob(path.Join(logDir, "zkocc.*"))
	if err != nil {
		t.Fatalf("filepath.Glob: %v", err)
	}

	if len(left) != 2 {
		// 151006 is still good, 131006 is the "current" log
		// (symlinked to zkocc.INFO), the rest should be
		// removed.
		t.Errorf("wrong number of files remain: want %v, got %v", 2, len(left))
	}

}
