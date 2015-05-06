// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package automation

import (
	"bytes"
	"os/exec"
	"strings"

	pb "github.com/youtube/vitess/go/vt/proto/automation"
)

// VtctlTask executes known vtctl commands. Output is preserved.
type VtctlTask struct {
}

func (t *VtctlTask) run(parameters map[string]string) ([]*pb.TaskContainer, string, error) {
	// TODO(mberlin): Use the vtctl code directly.
	args := []string{"-log_dir", "/tmp/vt/tmp"}
	args = append(args, strings.Split(parameters["command"], " ")...)
	cmd := exec.Command(
		"../bin/vtctl",
		args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()

	return nil, out.String(), err
}

func (t *VtctlTask) requiredParameters() []string {
	return []string{"command"}
}
