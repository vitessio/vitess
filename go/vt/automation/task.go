// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// Task implementations can be executed by the scheduler.
type Task interface {
	run(parameters map[string]*pb.Value) (*pb.Value, error)
}
