// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hook

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	vtenv "github.com/youtube/vitess/go/vt/env"
)

type Hook struct {
	Name       string
	Parameters []string
	ExtraEnv   map[string]string
}

type HookResult struct {
	ExitStatus int // HOOK_SUCCESS if it succeeded
	Stdout     string
	Stderr     string
}

// the hook will return a value between 0 and 255. 0 if it succeeds.
// so we have these additional values here for more information.
const (
	HOOK_SUCCESS                = 0
	HOOK_DOES_NOT_EXIST         = -1
	HOOK_STAT_FAILED            = -2
	HOOK_CANNOT_GET_EXIT_STATUS = -3
	HOOK_INVALID_NAME           = -4
	HOOK_VTROOT_ERROR           = -5
)

func NewHook(name string, params []string) *Hook {
	return &Hook{Name: name, Parameters: params}
}

func NewSimpleHook(name string) *Hook {
	return &Hook{Name: name}
}

func (hook *Hook) Execute() (result *HookResult) {
	result = &HookResult{}

	// also check for bad string here on the server side, to be sure
	if strings.Contains(hook.Name, "/") {
		result.ExitStatus = HOOK_INVALID_NAME
		result.Stderr = "Hooks cannot contains '/'\n"
		return result
	}

	// find our root
	root, err := vtenv.VtRoot()
	if err != nil {
		result.ExitStatus = HOOK_VTROOT_ERROR
		result.Stdout = "Cannot get VTROOT: " + err.Error() + "\n"
		return result
	}

	// see if the hook exists
	vthook := path.Join(root, "vthook", hook.Name)
	_, err = os.Stat(vthook)
	if err != nil {
		if os.IsNotExist(err) {
			result.ExitStatus = HOOK_DOES_NOT_EXIST
			result.Stdout = "Skipping missing hook: " + vthook + "\n"
			return result
		}

		result.ExitStatus = HOOK_STAT_FAILED
		result.Stderr = "Cannot stat hook: " + vthook + ": " + err.Error() + "\n"
		return result
	}

	// run it
	log.Infof("hook: executing hook: %v %v", vthook, strings.Join(hook.Parameters, " "))
	cmd := exec.Command(vthook, hook.Parameters...)
	if len(hook.ExtraEnv) > 0 {
		cmd.Env = os.Environ()
		for key, value := range hook.ExtraEnv {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
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

// Execute an optional hook, returns a printable error
func (hook *Hook) ExecuteOptional() error {
	hr := hook.Execute()
	if hr.ExitStatus == HOOK_DOES_NOT_EXIST {
		log.Infof("%v hook doesn't exist", hook.Name)
	} else if hr.ExitStatus != HOOK_SUCCESS {
		return fmt.Errorf("%v hook failed(%v): %v", hook.Name, hr.ExitStatus, hr.Stderr)
	}
	return nil
}

func (hr *HookResult) String() string {
	return jscfg.ToJson(hr)
}
