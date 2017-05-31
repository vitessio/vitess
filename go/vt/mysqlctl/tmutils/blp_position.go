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
