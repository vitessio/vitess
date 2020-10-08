/*
Copyright 2020 The Vitess Authors.

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
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// VtorcProcess is a test struct for running
// vtorc as a separate process for testing
type VtorcProcess struct {
	VtctlProcess
	LogDir    string
	ExtraArgs []string
	Config    string
	proc      *exec.Cmd
	exit      chan error
}

// Setup starts orc process with required arguements
func (orc *VtorcProcess) Setup() (err error) {

	/* minimal command line arguments:
	$ vtorc -topo_implementation etcd2 -topo_global_server_address localhost:2379 -topo_global_root /vitess/global
	-config config/orchestrator/default.json -alsologtostderr http
	*/
	orc.proc = exec.Command(
		orc.Binary,
		"-topo_implementation", orc.TopoImplementation,
		"-topo_global_server_address", orc.TopoGlobalAddress,
		"-topo_global_root", orc.TopoGlobalRoot,
		"-config", orc.Config,
	)
	if *isCoverage {
		orc.proc.Args = append(orc.proc.Args, "-test.coverprofile="+getCoveragePath("orc.out"))
	}

	orc.proc.Args = append(orc.proc.Args, orc.ExtraArgs...)
	orc.proc.Args = append(orc.proc.Args, "-alsologtostderr", "http")

	errFile, _ := os.Create(path.Join(orc.LogDir, "orc-stderr.txt"))
	orc.proc.Stderr = errFile

	orc.proc.Env = append(orc.proc.Env, os.Environ()...)

	log.Infof("Running orc with command: %v", strings.Join(orc.proc.Args, " "))

	err = orc.proc.Start()
	if err != nil {
		return
	}

	orc.exit = make(chan error)
	go func() {
		if orc.proc != nil {
			orc.exit <- orc.proc.Wait()
		}
	}()

	return nil
}

// TearDown shuts down the running vtorc service
func (orc *VtorcProcess) TearDown() error {
	if orc.proc == nil || orc.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	_ = orc.proc.Process.Signal(syscall.SIGTERM)

	select {
	case <-orc.exit:
		orc.proc = nil
		return nil

	case <-time.After(10 * time.Second):
		_ = orc.proc.Process.Kill()
		orc.proc = nil
		return <-orc.exit
	}
}
