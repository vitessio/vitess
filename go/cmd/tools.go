/*
Copyright 2019 The Vitess Authors

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
package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// DetachFromTerminalAndExit allows a command line program to detach from the terminal and continue running
// even if the parent process is terminated
func DetachFromTerminalAndExit() {
	args := os.Args[1:]
	i := 0
	for ; i < len(args); i++ {
		if strings.HasPrefix(args[i], "-detach") {
			args[i] = "-detach=false"
			break
		}
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Start()
	fmt.Println("[PID]", cmd.Process.Pid)
	os.Exit(0)
}
