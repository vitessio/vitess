/*
   Copyright 2014 Outbrain Inc.

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

package os

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

var EmptyEnv = []string{}

// CommandRun executes some text as a command. This is assumed to be
// text that will be run by a shell so we need to write out the
// command to a temporary file and then ask the shell to execute
// it, after which the temporary file is removed.
func CommandRun(commandText string, env []string, arguments ...string) error {
	// show the actual command we have been asked to run
	log.Infof("CommandRun(%v,%+v)", commandText, arguments)

	cmd, shellScript, err := generateShellScript(commandText, env, arguments...)
	defer os.Remove(shellScript)
	if err != nil {
		return log.Errore(err)
	}

	var waitStatus syscall.WaitStatus

	log.Infof("CommandRun/running: %s", strings.Join(cmd.Args, " "))
	cmdOutput, err := cmd.CombinedOutput()
	log.Infof("CommandRun: %s\n", string(cmdOutput))
	if err != nil {
		// Did the command fail because of an unsuccessful exit code
		if exitError, ok := err.(*exec.ExitError); ok {
			waitStatus = exitError.Sys().(syscall.WaitStatus)
			log.Errorf("CommandRun: failed. exit status %d", waitStatus.ExitStatus())
		}

		return log.Errore(fmt.Errorf("(%s) %s", err.Error(), cmdOutput))
	}

	// Command was successful
	waitStatus = cmd.ProcessState.Sys().(syscall.WaitStatus)
	log.Infof("CommandRun successful. exit status %d", waitStatus.ExitStatus())

	return nil
}

// generateShellScript generates a temporary shell script based on
// the given command to be executed, writes the command to a temporary
// file and returns the exec.Command which can be executed together
// with the script name that was created.
func generateShellScript(commandText string, env []string, arguments ...string) (*exec.Cmd, string, error) {
	shell := config.Config.ProcessesShellCommand

	commandBytes := []byte(commandText)
	tmpFile, err := ioutil.TempFile("", "orchestrator-process-cmd-")
	if err != nil {
		return nil, "", log.Errorf("generateShellScript() failed to create TempFile: %v", err.Error())
	}
	// write commandText to temporary file
	ioutil.WriteFile(tmpFile.Name(), commandBytes, 0640)
	shellArguments := append([]string{}, tmpFile.Name())
	shellArguments = append(shellArguments, arguments...)

	cmd := exec.Command(shell, shellArguments...)
	cmd.Env = env

	return cmd, tmpFile.Name(), nil
}
