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

package cluster

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// VtctldProcess is a generic handle for a running vtctld .
// It can be spawned manually
type VtctldProcess struct {
	Name                        string
	Binary                      string
	CommonArg                   VtctlProcess
	ServiceMap                  string
	BackupStorageImplementation string
	FileBackupStorageRoot       string
	LogDir                      string
	Port                        int
	GrpcPort                    int
	VerifyURL                   string
	Directory                   string

	proc *exec.Cmd
	exit chan error
}

// Setup starts vtctld process with required arguements
func (vtctld *VtctldProcess) Setup(cell string, extraArgs ...string) (err error) {
	_ = createDirectory(vtctld.LogDir, 0700)
	_ = createDirectory(path.Join(vtctld.Directory, "backups"), 0700)
	vtctld.proc = exec.Command(
		vtctld.Binary,
		"-enable_queries",
		"-topo_implementation", vtctld.CommonArg.TopoImplementation,
		"-topo_global_server_address", vtctld.CommonArg.TopoGlobalAddress,
		"-topo_global_root", vtctld.CommonArg.TopoGlobalRoot,
		"-cell", cell,
		"-workflow_manager_init",
		"-workflow_manager_use_election",
		"-service_map", vtctld.ServiceMap,
		"-backup_storage_implementation", vtctld.BackupStorageImplementation,
		"-file_backup_storage_root", vtctld.FileBackupStorageRoot,
		// hard-code these two soon-to-be deprecated drain values.
		"-wait_for_drain_sleep_rdonly", "1s",
		"-wait_for_drain_sleep_replica", "1s",
		// short online-ddl check interval to hasten tests
		"-online_ddl_check_interval", "2s",
		"-log_dir", vtctld.LogDir,
		"-port", fmt.Sprintf("%d", vtctld.Port),
		"-grpc_port", fmt.Sprintf("%d", vtctld.GrpcPort),
	)
	if *isCoverage {
		vtctld.proc.Args = append(vtctld.proc.Args, "-test.coverprofile="+getCoveragePath("vtctld.out"))
	}
	vtctld.proc.Args = append(vtctld.proc.Args, extraArgs...)

	errFile, _ := os.Create(path.Join(vtctld.LogDir, "vtctld-stderr.txt"))
	vtctld.proc.Stderr = errFile

	vtctld.proc.Env = append(vtctld.proc.Env, os.Environ()...)

	log.Infof("Starting vtctld with command: %v", strings.Join(vtctld.proc.Args, " "))

	err = vtctld.proc.Start()
	if err != nil {
		return
	}

	vtctld.exit = make(chan error)
	go func() {
		vtctld.exit <- vtctld.proc.Wait()
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vtctld.IsHealthy() {
			return nil
		}
		select {
		case err := <-vtctld.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vtctld.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", vtctld.Name, <-vtctld.exit)
}

func createDirectory(dirName string, mode os.FileMode) error {
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		return os.Mkdir(dirName, mode)
	}
	return nil
}

// IsHealthy function checks if vtctld process is up and running
func (vtctld *VtctldProcess) IsHealthy() bool {
	resp, err := http.Get(vtctld.VerifyURL)
	if err != nil {
		return false
	}
	if resp.StatusCode == 200 {
		return true
	}
	return false
}

// TearDown shutdowns the running vtctld service
func (vtctld *VtctldProcess) TearDown() error {
	if vtctld.proc == nil || vtctld.exit == nil {
		return nil
	}

	// Attempt graceful shutdown with SIGTERM first
	vtctld.proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-vtctld.exit:
		vtctld.proc = nil
		return err

	case <-time.After(10 * time.Second):
		vtctld.proc.Process.Kill()
		vtctld.proc = nil
		return <-vtctld.exit
	}
}

// VtctldProcessInstance returns a VtctlProcess handle for vtctl process
// configured with the given Config.
// The process must be manually started by calling setup()
func VtctldProcessInstance(httpPort int, grpcPort int, topoPort int, hostname string, tmpDirectory string) *VtctldProcess {
	vtctl := VtctlProcessInstance(topoPort, hostname)
	vtctld := &VtctldProcess{
		Name:                        "vtctld",
		Binary:                      "vtctld",
		CommonArg:                   *vtctl,
		ServiceMap:                  "grpc-vtctl",
		BackupStorageImplementation: "file",
		FileBackupStorageRoot:       path.Join(os.Getenv("VTDATAROOT"), "/backups"),
		LogDir:                      tmpDirectory,
		Port:                        httpPort,
		GrpcPort:                    grpcPort,
		Directory:                   os.Getenv("VTDATAROOT"),
	}
	vtctld.VerifyURL = fmt.Sprintf("http://%s:%d/debug/vars", hostname, vtctld.Port)
	return vtctld
}
