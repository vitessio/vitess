/*
Copyright 2017 Google Inc.

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

package hook

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"

	vtenv "vitess.io/vitess/go/vt/env"
	"vitess.io/vitess/go/vt/log"
)

// Hook is the input structure for this library.
type Hook struct {
	Name       string
	Parameters []string
	ExtraEnv   map[string]string
}

// HookResult is returned by the Execute method.
type HookResult struct {
	ExitStatus int // HOOK_SUCCESS if it succeeded
	Stdout     string
	Stderr     string
}

// The hook will return a value between 0 and 255. 0 if it succeeds.
// So we have these additional values here for more information.
const (
	// HOOK_SUCCESS is returned when the hook worked.
	HOOK_SUCCESS = 0

	// HOOK_DOES_NOT_EXIST is returned when the hook cannot be found.
	HOOK_DOES_NOT_EXIST = -1

	// HOOK_STAT_FAILED is returned when the hook exists, but stat
	// on it fails.
	HOOK_STAT_FAILED = -2

	// HOOK_CANNOT_GET_EXIT_STATUS is returned when after
	// execution, we fail to get the exit code for the hook.
	HOOK_CANNOT_GET_EXIT_STATUS = -3

	// HOOK_INVALID_NAME is returned if a hook has an invalid name.
	HOOK_INVALID_NAME = -4

	// HOOK_VTROOT_ERROR is returned if VTROOT is not set properly.
	HOOK_VTROOT_ERROR = -5

	// HOOK_GENERIC_ERROR is returned for unknown errors.
	HOOK_GENERIC_ERROR = -6
)

// WaitFunc is a return type for the Pipe methods.
// It returns the process stderr and an error, if any.
type WaitFunc func() (string, error)

// NewHook returns a Hook object with the provided name and params.
func NewHook(name string, params []string) *Hook {
	return &Hook{Name: name, Parameters: params}
}

// NewSimpleHook returns a Hook object with just a name.
func NewSimpleHook(name string) *Hook {
	return &Hook{Name: name}
}

// NewHookWithEnv returns a Hook object with the provided name, params and ExtraEnv.
func NewHookWithEnv(name string, params []string, env map[string]string) *Hook {
	return &Hook{Name: name, Parameters: params, ExtraEnv: env}
}

// findHook trie to locate the hook, and returns the exec.Cmd for it.
func (hook *Hook) findHook() (*exec.Cmd, int, error) {
	// Check the hook path.
	if strings.Contains(hook.Name, "/") {
		return nil, HOOK_INVALID_NAME, fmt.Errorf("hook cannot contain '/'")
	}

	// Find our root.
	root, err := vtenv.VtRoot()
	if err != nil {
		return nil, HOOK_VTROOT_ERROR, fmt.Errorf("cannot get VTROOT: %v", err)
	}

	// See if the hook exists.
	vthook := path.Join(root, "vthook", hook.Name)
	_, err = os.Stat(vthook)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, HOOK_DOES_NOT_EXIST, fmt.Errorf("missing hook %v", vthook)
		}

		return nil, HOOK_STAT_FAILED, fmt.Errorf("cannot stat hook %v: %v", vthook, err)
	}

	// Configure the command.
	log.Infof("hook: executing hook: %v %v", vthook, strings.Join(hook.Parameters, " "))
	cmd := exec.Command(vthook, hook.Parameters...)
	if len(hook.ExtraEnv) > 0 {
		cmd.Env = os.Environ()
		for key, value := range hook.ExtraEnv {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}

	return cmd, HOOK_SUCCESS, nil
}

// Execute tries to execute the Hook and returns a HookResult.
func (hook *Hook) Execute() (result *HookResult) {
	result = &HookResult{}

	// Find the hook.
	cmd, status, err := hook.findHook()
	if err != nil {
		result.ExitStatus = status
		result.Stderr = err.Error() + "\n"
		return result
	}

	// Run it.
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	if err == nil {
		result.ExitStatus = HOOK_SUCCESS
	} else {
		if cmd.ProcessState != nil && cmd.ProcessState.Sys() != nil {
			result.ExitStatus = cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
		} else {
			result.ExitStatus = HOOK_CANNOT_GET_EXIT_STATUS
		}
		result.Stderr += "ERROR: " + err.Error() + "\n"
	}

	log.Infof("hook: result is %v", result.String())

	return result
}

// ExecuteOptional executes an optional hook, logs if it doesn't
// exist, and returns a printable error.
func (hook *Hook) ExecuteOptional() error {
	hr := hook.Execute()
	switch hr.ExitStatus {
	case HOOK_DOES_NOT_EXIST:
		log.Infof("%v hook doesn't exist", hook.Name)
	case HOOK_VTROOT_ERROR:
		log.Infof("VTROOT not set, so %v hook doesn't exist", hook.Name)
	case HOOK_SUCCESS:
		// nothing to do here
	default:
		return fmt.Errorf("%v hook failed(%v): %v", hook.Name, hr.ExitStatus, hr.Stderr)
	}
	return nil
}

// ExecuteAsWritePipe will execute the hook as in a Unix pipe,
// directing output to the provided writer. It will return:
// - an io.WriteCloser to write data to.
// - a WaitFunc method to call to wait for the process to exit,
// that returns stderr and the cmd.Wait() error.
// - an error code and an error if anything fails.
func (hook *Hook) ExecuteAsWritePipe(out io.Writer) (io.WriteCloser, WaitFunc, int, error) {
	// Find the hook.
	cmd, status, err := hook.findHook()
	if err != nil {
		return nil, nil, status, err
	}

	// Configure the process's stdin, stdout, and stderr.
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, HOOK_GENERIC_ERROR, fmt.Errorf("Failed to configure stdin: %v", err)
	}
	cmd.Stdout = out
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// Start the process.
	err = cmd.Start()
	if err != nil {
		status = HOOK_CANNOT_GET_EXIT_STATUS
		if cmd.ProcessState != nil && cmd.ProcessState.Sys() != nil {
			status = cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
		}
		return nil, nil, status, err
	}

	// And return
	return in, func() (string, error) {
		err := cmd.Wait()
		return stderr.String(), err
	}, HOOK_SUCCESS, nil
}

// ExecuteAsReadPipe will execute the hook as in a Unix pipe, reading
// from the provided reader. It will return:
// - an io.Reader to read piped data from.
// - a WaitFunc method to call to wait for the process to exit, that
// returns stderr and the Wait() error.
// - an error code and an error if anything fails.
func (hook *Hook) ExecuteAsReadPipe(in io.Reader) (io.Reader, WaitFunc, int, error) {
	// Find the hook.
	cmd, status, err := hook.findHook()
	if err != nil {
		return nil, nil, status, err
	}

	// Configure the process's stdin, stdout, and stderr.
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, HOOK_GENERIC_ERROR, fmt.Errorf("Failed to configure stdout: %v", err)
	}
	cmd.Stdin = in
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	// Start the process.
	err = cmd.Start()
	if err != nil {
		status = HOOK_CANNOT_GET_EXIT_STATUS
		if cmd.ProcessState != nil && cmd.ProcessState.Sys() != nil {
			status = cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
		}
		return nil, nil, status, err
	}

	// And return
	return out, func() (string, error) {
		err := cmd.Wait()
		return stderr.String(), err
	}, HOOK_SUCCESS, nil
}

// String returns a printable version of the HookResult
func (hr *HookResult) String() string {
	result := "result: "
	switch hr.ExitStatus {
	case HOOK_SUCCESS:
		result += "HOOK_SUCCESS"
	case HOOK_DOES_NOT_EXIST:
		result += "HOOK_DOES_NOT_EXIST"
	case HOOK_STAT_FAILED:
		result += "HOOK_STAT_FAILED"
	case HOOK_CANNOT_GET_EXIT_STATUS:
		result += "HOOK_CANNOT_GET_EXIT_STATUS"
	case HOOK_INVALID_NAME:
		result += "HOOK_INVALID_NAME"
	case HOOK_VTROOT_ERROR:
		result += "HOOK_VTROOT_ERROR"
	default:
		result += fmt.Sprintf("exit(%v)", hr.ExitStatus)
	}
	if hr.Stdout != "" {
		result += "\nstdout:\n" + hr.Stdout
	}
	if hr.Stderr != "" {
		result += "\nstderr:\n" + hr.Stderr
	}
	return result
}
