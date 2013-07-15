// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/vt/naming"
	"code.google.com/p/vitess/go/zk"
)

// Functions for extracting and deriving zk paths.

// Tablet aliases are the nodes that point into /vt/tablets/<uid> from the keyspace
// Note that these are *global*
func IsTabletReplicationPath(zkReplicationPath string) bool {
	_, _, err := ParseTabletReplicationPath(zkReplicationPath)
	return err == nil
}

func ParseTabletReplicationPath(zkReplicationPath string) (cell string, uid uint32, err error) {
	cell, err = zk.ZkCellFromZkPath(zkReplicationPath)
	if err != nil {
		return
	}
	if cell != "global" {
		return "", 0, fmt.Errorf("invalid replication path cell, expected global: %v", zkReplicationPath)
	}

	// /zk/cell/vt/keyspaces/<k>/shards/<s>/alias/...
	pathParts := strings.Split(zkReplicationPath, "/")
	shardIdx := -1
	for i, part := range pathParts {
		if part == "shards" {
			shardIdx = i
			break
		}
	}
	if shardIdx == -1 || shardIdx+2 >= len(pathParts) {
		return "", 0, fmt.Errorf("invalid replication path %v", zkReplicationPath)
	}

	nameParts := strings.Split(path.Base(zkReplicationPath), "-")
	if len(nameParts) != 2 {
		return "", 0, fmt.Errorf("invalid replication path %v", zkReplicationPath)
	}
	cell = nameParts[0]
	uid, err = ParseUid(nameParts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid replication path uid %v: %v", zkReplicationPath, err)
	}
	return
}

func tabletUidStr(uid uint32) string {
	return fmt.Sprintf("%010d", uid)
}

func ParseUid(value string) (uint32, error) {
	uid, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad tablet uid %v", err)
	}
	return uint32(uid), nil
}

func fmtAlias(cell string, uid uint32) string {
	return fmt.Sprintf("%v-%v", cell, tabletUidStr(uid))
}

// FIXME(msolomon) This method doesn't take into account the vt subtree.
func TabletPathForAlias(alias naming.TabletAlias) string {
	return fmt.Sprintf("/zk/%v/vt/tablets/%v", alias.Cell, tabletUidStr(alias.Uid))
}
