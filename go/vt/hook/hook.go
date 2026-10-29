/*
Copyright 2019 The Vitess Authors.

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
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/fileutil"
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

// hookWaitDelay bounds how long ExecuteOptionalContext waits for a hook's
// output after the hook exits or is killed.
const hookWaitDelay = time.Second

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

	// HOOK_TIMEOUT_ERROR is returned when a CommandContext has its context
	// become done before the command terminates.
	HOOK_TIMEOUT_ERROR = -7
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

// findHook tries to locate the hook, and returns the exec.Cmd for it.
func (hook *Hook) findHook(ctx context.Context) (*exec.Cmd, int, error) {
	// Find our root.
	root, err := vtenv.VtRoot()
	if err != nil {
		return nil, HOOK_VTROOT_ERROR, fmt.Errorf("cannot get VTROOT: %v", err)
	}

	// See if the hook exists.
	vthook, err := fileutil.SafePathJoin(filepath.Join(root, "vthook"), hook.Name)
	if err != nil {
		return nil, HOOK_INVALID_NAME, fmt.Errorf("invalid hook name %q: %v", hook.Name, err)
	}
	_, err = os.Stat(vthook)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, HOOK_DOES_NOT_EXIST, fmt.Errorf("missing hook %v", vthook)
		}

		return nil, HOOK_STAT_FAILED, fmt.Errorf("cannot stat hook %v: %v", vthook, err)
	}

	// Configure the command.
	log.Info(fmt.Sprintf("hook: executing hook: %v %v", vthook, strings.Join(hook.Parameters, " ")))
	cmd := exec.CommandContext(ctx, vthook, hook.Parameters...)
	if len(hook.ExtraEnv) > 0 {
		cmd.Env = os.Environ()
		for key, value := range hook.ExtraEnv {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}

	return cmd, HOOK_SUCCESS, nil
}

// ExecuteContext tries to execute the Hook with the given context and returns a HookResult.
func (hook *Hook) ExecuteContext(ctx context.Context) *HookResult {
	return hook.execute(ctx, 0)
}

// execute runs the hook. A child the hook started can keep its output open
// after the hook exits or is killed. A zero waitDelay waits for that output to
// close; otherwise execute stops waiting after waitDelay, and closes the
// output, so a child that writes to it later fails or is killed by SIGPIPE.
func (hook *Hook) execute(ctx context.Context, waitDelay time.Duration) (result *HookResult) {
	result = &HookResult{}

	// Find the hook.
	cmd, status, err := hook.findHook(ctx)
	if err != nil {
		result.ExitStatus = status
		result.Stderr = err.Error() + "\n"
		return result
	}

	// Run it.
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.WaitDelay = waitDelay

	start := time.Now()
	err = cmd.Run()
	duration := time.Since(start)

	result.Stdout = stdout.String()
	result.Stderr = stderr.String()

	defer func() {
		log.Info(fmt.Sprintf("hook: result is %v", result.String()))
	}()

	if err == nil {
		result.ExitStatus = HOOK_SUCCESS
		return result
	}

	// A hook that exited on its own keeps its exit status, even if its
	// context ended while a child held its output open.
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		result.ExitStatus = cmd.ProcessState.ExitCode()
		if result.ExitStatus == HOOK_SUCCESS {
			// Only the wait for the output failed.
			result.Stderr += "WARNING: the output may be incomplete: " + err.Error() + "\n"
		} else {
			result.Stderr += "ERROR: " + err.Error() + "\n"
		}
		return result
	}

	if ctx.Err() != nil {
		// When (exec.Cmd).Run hits a context that expired or was canceled, the process is killed.
		// This means:
		// 	1. cmd.ProcessState.Exited() is false.
		//	2. cmd.ProcessState.ExitCode() is -1.
		// [ref]: https://golang.org/pkg/os/#ProcessState.ExitCode
		//
		// Therefore, we need to catch this error specifically, and set result.ExitStatus to
		// HOOK_TIMEOUT_ERROR, because just using ExitStatus will result in HOOK_DOES_NOT_EXIST,
		// which would be wrong. Since we're already doing some custom handling, we'll also include
		// the amount of time the command was running in the error string, in case that is helpful.
		result.ExitStatus = HOOK_TIMEOUT_ERROR
		result.Stderr += fmt.Sprintf("ERROR: (after %s) %s\n", duration, err)
		return result
	}

	if cmd.ProcessState != nil {
		status := cmd.ProcessState.Sys().(syscall.WaitStatus)
		result.ExitStatus = status.ExitStatus()
		if status.Signaled() {
			// The exit status of a signaled hook is -1, which is HOOK_DOES_NOT_EXIST.
			result.ExitStatus = HOOK_GENERIC_ERROR
		}
	} else {
		result.ExitStatus = HOOK_CANNOT_GET_EXIT_STATUS
	}
	result.Stderr += "ERROR: " + err.Error() + "\n"

	return result
}

// Execute tries to execute the Hook and returns a HookResult.
func (hook *Hook) Execute() (result *HookResult) {
	return hook.ExecuteContext(context.Background())
}

// ExecuteOptional executes an optional hook, logs if it doesn't
// exist, and returns a printable error.
func (hook *Hook) ExecuteOptional() error {
	return hook.optionalError(hook.Execute())
}

// ExecuteOptionalContext is ExecuteOptional, but the hook is killed when ctx
// ends, and it waits at most hookWaitDelay for the hook's output once the hook
// exits or is killed. A child the hook started that writes to its output after
// that fails or is killed by SIGPIPE.
func (hook *Hook) ExecuteOptionalContext(ctx context.Context) error {
	return hook.optionalError(hook.execute(ctx, hookWaitDelay))
}

func (hook *Hook) optionalError(hr *HookResult) error {
	switch hr.ExitStatus {
	case HOOK_DOES_NOT_EXIST:
		log.Info(fmt.Sprintf("%v hook doesn't exist", hook.Name))
	case HOOK_VTROOT_ERROR:
		log.Info(fmt.Sprintf("VTROOT not set, so %v hook doesn't exist", hook.Name))
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
	cmd, status, err := hook.findHook(context.Background())
	if err != nil {
		return nil, nil, status, err
	}

	// Configure the process's stdin, stdout, and stderr.
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, HOOK_GENERIC_ERROR, fmt.Errorf("failed to configure stdin: %v", err)
	}
	cmd.Stdout = out
	var stderr strings.Builder
	cmd.Stderr = &stderr

	// Start the process.
	err = cmd.Start()
	if err != nil {
		status = HOOK_CANNOT_GET_EXIT_STATUS
		if cmd.ProcessState != nil {
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
	cmd, status, err := hook.findHook(context.Background())
	if err != nil {
		return nil, nil, status, err
	}

	// Configure the process's stdin, stdout, and stderr.
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, HOOK_GENERIC_ERROR, fmt.Errorf("failed to configure stdout: %v", err)
	}
	cmd.Stdin = in
	var stderr strings.Builder
	cmd.Stderr = &stderr

	// Start the process.
	err = cmd.Start()
	if err != nil {
		status = HOOK_CANNOT_GET_EXIT_STATUS
		if cmd.ProcessState != nil {
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
