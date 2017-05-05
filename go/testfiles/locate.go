/*
Copyright 2017 Google Inc.

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
