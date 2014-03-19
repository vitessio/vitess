// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testfiles

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
)

func Locate(filename string) string {
	vtroot := os.Getenv("VTROOT")
	if vtroot == "" {
		panic(fmt.Errorf("VTROOT is not set"))
	}
	return path.Join(vtroot, "data", "test", filename)
}

func Glob(pattern string) []string {
	vtroot := os.Getenv("VTROOT")
	if vtroot == "" {
		panic(fmt.Errorf("VTROOT is not set"))
	}
	resolved := path.Join(vtroot, "data", "test", pattern)
	out, err := filepath.Glob(resolved)
	if err != nil {
		panic(err)
	}
	return out
}
