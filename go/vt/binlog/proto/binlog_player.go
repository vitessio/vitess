// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

// Binlog server / player replication structures

import pbt "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"

// FindBlpPositionByID returns the BlpPosition with the given id, or error
func FindBlpPositionByID(list []*pbt.BlpPosition, uid uint32) *pbt.BlpPosition {
	for _, pos := range list {
		if pos.Uid == uid {
			return pos
		}
	}
	return nil
}
