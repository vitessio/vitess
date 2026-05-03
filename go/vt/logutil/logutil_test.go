/*
Copyright 2019 The Vitess Authors.

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

package logutil

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsing(t *testing.T) {
	path := []string{
		"/tmp/something.foo/zkocc.goedel.szopa.log.INFO.20130806-151006.10530",
		"/tmp/something.foo/zkocc.goedel.szopa.test.log.ERROR.20130806-151006.10530",
	}

	for _, filepath := range path {
		ts, err := parseCreatedTimestamp(filepath)
		require.NoError(t, err)
		want := time.Date(2013, 8, 6, 15, 10, 0o6, 0, time.Now().Location())
		assert.Equal(t, want, ts)
	}
}

func TestPurgeByCtime(t *testing.T) {
	logDir := path.Join(os.TempDir(), fmt.Sprintf("%v-%v", os.Args[0], os.Getpid()))
	require.NoError(t, os.MkdirAll(logDir, 0o777))
	defer os.RemoveAll(logDir)

	now := time.Date(2013, 8, 6, 15, 10, 0o6, 0, time.Now().Location())
	files := []string{
		"zkocc.goedel.szopa.log.INFO.20130806-121006.10530",
		"zkocc.goedel.szopa.log.INFO.20130806-131006.10530",
		"zkocc.goedel.szopa.log.INFO.20130806-141006.10530",
		"zkocc.goedel.szopa.log.INFO.20130806-151006.10530",
	}

	for _, file := range files {
		_, err := os.Create(path.Join(logDir, file))
		require.NoError(t, err)
	}
	require.NoError(t, os.Symlink(files[1], path.Join(logDir, "zkocc.INFO")))

	purgeLogsOnce(now, logDir, "zkocc", 30*time.Minute, 0)

	left, err := filepath.Glob(path.Join(logDir, "zkocc.*"))
	require.NoError(t, err)

	// 131006 is current
	// 151006 is within 30 min
	// symlink remains
	// the rest should be removed.
	assert.Len(t, left, 3)
}

func TestPurgeByMtime(t *testing.T) {
	logDir := path.Join(os.TempDir(), fmt.Sprintf("%v-%v", os.Args[0], os.Getpid()))
	require.NoError(t, os.MkdirAll(logDir, 0o777))
	defer os.RemoveAll(logDir)
	createFileWithMtime := func(filename, mtimeStr string) {
		mtime, err := time.Parse(time.RFC3339, mtimeStr)
		require.NoError(t, err)
		filepath := path.Join(logDir, filename)
		_, err = os.Create(filepath)
		require.NoError(t, err)
		require.NoError(t, os.Chtimes(filepath, mtime, mtime))
	}
	now := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	filenameMtimeMap := map[string]string{
		"vtadam.localhost.vitess.log.INFO.20200101-113000.00000": "2020-01-01T11:30:00.000Z",
		"vtadam.localhost.vitess.log.INFO.20200101-100000.00000": "2020-01-01T10:00:00.000Z",
		"vtadam.localhost.vitess.log.INFO.20200101-090000.00000": "2020-01-01T09:00:00.000Z",
		"vtadam.localhost.vitess.log.INFO.20200101-080000.00000": "2020-01-01T08:00:00.000Z",
	}
	for filename, mtimeStr := range filenameMtimeMap {
		createFileWithMtime(filename, mtimeStr)
	}

	// Create vtadam.INFO symlink to 100000. This is a contrived example in that
	// current log (100000) is not the latest log (113000). This will not happen
	// IRL but it helps us test edge cases of purging by mtime.
	require.NoError(t, os.Symlink("vtadam.localhost.vitess.log.INFO.20200101-100000.00000", path.Join(logDir, "vtadam.INFO")))

	purgeLogsOnce(now, logDir, "vtadam", 0, 1*time.Hour)

	left, err := filepath.Glob(path.Join(logDir, "vtadam.*"))
	require.NoError(t, err)

	// 1. 113000 is within 1 hour
	// 2. 100000 is current (vtadam.INFO)
	// 3. vtadam.INFO symlink remains
	// rest are removed
	assert.Len(t, left, 3)
}
