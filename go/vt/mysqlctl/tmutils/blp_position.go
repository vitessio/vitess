// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tmutils

// This file contains helper methods for dealing with the proto3 data
// structures related to binlog playback.

import tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"

// FindBlpPositionByID returns the BlpPosition with the given id, or error
func FindBlpPositionByID(list []*tabletmanagerdatapb.BlpPosition, uid uint32) *tabletmanagerdatapb.BlpPosition {
	for _, pos := range list {
		if pos.Uid == uid {
			return pos
		}
	}
	return nil
}
