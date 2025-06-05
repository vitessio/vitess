/*
Copyright 2025 The Vitess Authors.

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

package servenv

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	fmt.Println("Building test Docker image...")
	build := exec.Command("docker", "build", "-f", "docker/test/Dockerfile.metrics", "-t", "metrics:test", ".")
	build.Dir = "../../../.."
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build Docker image: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestReportCpuInDocker(t *testing.T) {
	cmd := exec.Command("docker", "run", "--rm", "metrics:test")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	require.NoError(t, err, "Docker test container failed")
}
