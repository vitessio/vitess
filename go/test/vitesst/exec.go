/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"context"
	"fmt"
	"io"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// containerExec runs a command inside a container and returns its exit code
// and combined stdout+stderr output. It blocks until the command exits.
func containerExec(ctx context.Context, ctr testcontainers.Container, cmd []string, opts ...tcexec.ProcessOption) (int, string, error) {
	opts = append(opts, tcexec.Multiplexed())

	exitCode, reader, err := ctr.Exec(ctx, cmd, opts...)
	if err != nil {
		return 0, "", fmt.Errorf("exec %v: %w", cmd, err)
	}

	output, err := io.ReadAll(reader)
	if err != nil {
		return exitCode, "", fmt.Errorf("reading output of exec %v: %w", cmd, err)
	}

	return exitCode, string(output), nil
}

// mustExec runs a command inside a container and fails on non-zero exit.
func mustExec(ctx context.Context, ctr testcontainers.Container, cmd []string, opts ...tcexec.ProcessOption) (string, error) {
	exitCode, output, err := containerExec(ctx, ctr, cmd, opts...)
	if err != nil {
		return "", err
	}
	if exitCode != 0 {
		return output, fmt.Errorf("exec %v failed with exit code %d: %s", cmd, exitCode, output)
	}
	return output, nil
}

// writeContainerFile writes content to a path inside a running container by
// exec'ing a shell. Unlike CopyToContainer, an exec runs inside the
// container's mount namespace, so writes land in tmpfs mounts such as
// /vt/vtdataroot instead of the shadowed image layer underneath them.
func writeContainerFile(ctx context.Context, ctr testcontainers.Container, path, content string) error {
	script := fmt.Sprintf("printf '%%s' %s > %s", shellQuote(content), path)
	if _, err := mustExec(ctx, ctr, []string{"bash", "-c", script}); err != nil {
		return fmt.Errorf("writing %d bytes to container path %s: %w", len(content), path, err)
	}
	return nil
}
