/*
Copyright 2023 The Vitess Authors.

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

// Package zkfilepath provides filepath utilities specialized to zookeeper.
package zkfilepath

import (
	"fmt"
	"path"
	"strings"

	"github.com/z-division/go-zookeeper/zk"

	"vitess.io/vitess/go/vt/topo/zk2topo"
)

const (
	TimeFmt      = "2006-01-02 15:04:05"
	TimeFmtMicro = "2006-01-02 15:04:05.000000"
)

// Clean returns the shortest path name of a zookeeper path after trimming
// trailing slashes.
func Clean(zkPath string) string {
	if zkPath != "/" {
		zkPath = strings.TrimSuffix(zkPath, "/")
	}

	return path.Clean(zkPath)
}

// Format returns a path formatted to a canonical string.
func Format(stat *zk.Stat, zkPath string, showFullPath bool, longListing bool) string {
	var name, perms string

	if !showFullPath {
		name = path.Base(zkPath)
	} else {
		name = zkPath
	}

	if longListing {
		if stat.NumChildren > 0 {
			// FIXME(msolomon) do permissions check?
			perms = "drwxrwxrwx"
			if stat.DataLength > 0 {
				// give a visual indication that this node has data as well as children
				perms = "nrw-rw-rw-"
			}
		} else if stat.EphemeralOwner != 0 {
			perms = "erw-rw-rw-"
		} else {
			perms = "-rw-rw-rw-"
		}
		// always print the Local version of the time. zookeeper's
		// go / C library would return a local time anyway, but
		// might as well be sure.
		return fmt.Sprintf("%v %v %v % 8v % 20v %v\n", perms, "zk", "zk", stat.DataLength, zk2topo.Time(stat.Mtime).Local().Format(TimeFmt), name)
	} else {
		return fmt.Sprintf("%v\n", name)
	}
}
