// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import "strconv"
import "sync/atomic"

// IDGenerator generates unique task and cluster operation IDs.
type IDGenerator struct {
	counter int64
}

// GetNextID returns an ID which wasn't returned before.
func (ig *IDGenerator) GetNextID() string {
	return strconv.FormatInt(atomic.AddInt64(&ig.counter, 1), 10)
}
