/*
Copyright 2017 Google Inc.

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

// Package eventtoken includes utility methods for event token
// handling.
package eventtoken

import (
	"github.com/youtube/vitess/go/mysql"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// Fresher compares two event tokens.  It returns a negative number if
// ev1<ev2, zero if they're equal, and a positive number if
// ev1>ev2. In case of doubt (we don't have enough information to know
// for sure), it returns a negative number.
func Fresher(ev1, ev2 *querypb.EventToken) int {
	if ev1 == nil || ev2 == nil {
		// Either one is nil, we don't know.
		return -1
	}

	if ev1.Timestamp != ev2.Timestamp {
		// The timestamp is enough to set them apart.
		return int(ev1.Timestamp - ev2.Timestamp)
	}

	if ev1.Shard != "" && ev1.Shard == ev2.Shard {
		// They come from the same shard. See if we have positions.
		if ev1.Position == "" || ev2.Position == "" {
			return -1
		}

		// We can parse them.
		pos1, err := mysql.DecodePosition(ev1.Position)
		if err != nil {
			return -1
		}
		pos2, err := mysql.DecodePosition(ev2.Position)
		if err != nil {
			return -1
		}

		// Then compare.
		if pos1.Equal(pos2) {
			return 0
		}
		if pos1.AtLeast(pos2) {
			return 1
		}
		return -1
	}

	// We do not know.
	return -1
}
