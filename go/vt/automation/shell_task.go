// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"bytes"
	"os/exec"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// ShellTask allows to execute shell commands and returns the command's stdout.
type ShellTask struct {
}

func (t *ShellTask) run(parameters map[string]*pb.Value) (*pb.Value, error) {
	cmd := exec.Command(parameters["command"].Value[0], parameters["command"].Value[1:]...)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return nil, err
	}

	// TODO(mberlin): Output will contain '\n'. Strip it to avoid that it gets propagated?
	return &pb.Value{
		Value: []string{out.String()},
	}, nil
}
