// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"testing"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

func TestShellTask(t *testing.T) {
	task := ShellTask{}
	output, _ := task.run(
		map[string]*pb.Value{
			"command": &pb.Value{
				Value: []string{"/bin/echo", "test"},
			},
		})
	if output.Value[0] != "test\n" {
		t.Errorf("Wrong output: got: '%v' want: '%v'", output.Value[0], "test\n")
	}
}

// TODO(mberlin): Add test for error path.
