/*
Copyright 2024 The Vitess Authors.

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

package cluster

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
)

type processInfo struct {
	proc   *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func newCommand(name string, arg ...string) *processInfo {
	pi := &processInfo{
		proc: exec.Command(name, arg...),
	}
	pi.proc.Stdout = &pi.stdout
	pi.proc.Stderr = &pi.stderr
	return pi
}

func (pi *processInfo) addArgs(arg ...string) {
	pi.proc.Args = append(pi.proc.Args, arg...)
}

func (pi *processInfo) getArgs() []string {
	return pi.proc.Args
}

func (pi *processInfo) addEnv(arg ...string) {
	pi.proc.Env = append(pi.proc.Env, arg...)
}

func (pi *processInfo) failed(err error) error {
	if err == nil {
		return nil
	}
	message := "failed to run %v: \n%w"
	if out := pi.stdout.String(); out != "" {
		message += fmt.Sprintf("\nstdout: %s", out)
	}
	if out := pi.stderr.String(); out != "" {
		message += fmt.Sprintf("\nstderr: %s", out)
	}

	return fmt.Errorf(message, pi.proc.Args, err)
}

func (pi *processInfo) start() error {
	err := pi.proc.Start()
	return pi.failed(err)
}

func (pi *processInfo) wait() error {
	err := pi.proc.Wait()
	return pi.failed(err)
}

func (pi *processInfo) process() *os.Process {
	return pi.proc.Process
}

func (pi *processInfo) run() error {
	err := pi.proc.Run()
	return pi.failed(err)
}
