//go:build linux

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
	"flag"
	"fmt"
	"os"

	"vitess.io/vitess/go/test/endtoend/tabletmanager/disk_health_monitor/testfs"
)

func main() {
	mountPoint := flag.String("mount", "", "directory to mount the FUSE filesystem on")
	backing := flag.String("backing", "", "directory whose contents are mirrored through the mount")
	flag.Parse()

	if err := testfs.Run(*mountPoint, *backing); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
