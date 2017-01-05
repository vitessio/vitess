// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package testfiles locates test files within the Vitess directory tree.
// It also handles test port allocation.
package testfiles

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
)

// Locate returns a file path that came from $VTROOT/data/test.
func Locate(filename string) string {
	vtroot := os.Getenv("VTROOT")
	if vtroot == "" {
		panic(fmt.Errorf("VTROOT is not set"))
	}
	return path.Join(vtroot, "data", "test", filename)
}

// Glob returns all files matching a pattern in $VTROOT/data/test.
func Glob(pattern string) []string {
	vtroot := os.Getenv("VTROOT")
	if vtroot == "" {
		panic(fmt.Errorf("VTROOT is not set"))
	}
	dir := path.Join(vtroot, "data", "test")
	if exists, err := exists(dir); !exists {
		panic(err)
	}
	resolved := path.Join(dir, pattern)
	out, err := filepath.Glob(resolved)
	if err != nil {
		panic(err)
	}
	return out
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, err
	}
	return false, err
}
