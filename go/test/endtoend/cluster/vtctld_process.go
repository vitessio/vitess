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
	"strconv"
	"strings"
	"syscall"
	"time"

	vtutils "vitess.io/vitess/go/vt/utils"

	"vitess.io/vitess/go/vt/log"
)

// VtctldProcess is a generic handle for a running vtctld .
// It can be spawned manually
type VtctldProcess struct {
	VtProcess
	ServiceMap                  string
	BackupStorageImplementation string
	FileBackupStorageRoot       string
	LogDir                      string
	ErrorLog                    string
	Port                        int
	GrpcPort                    int
	VerifyURL                   string
	Directory                   string

	proc *exec.Cmd
	exit chan error
}

// Setup starts vtctld process with required arguements
func (vtctld *VtctldProcess) Setup(cell string, extraArgs ...string) (err error) {
	vtctldVer, err := GetMajorVersion(vtctld.Binary)
	if err != nil {
		return err
	}
	_ = createDirectory(vtctld.LogDir, 0700)
	_ = createDirectory(path.Join(vtctld.Directory, "backups"), 0700)
	vtctld.proc = exec.Command(
		vtctld.Binary,
		//TODO: Remove underscore(_) flags in v25, replace them with dashed(-) notation
		"--topo_implementation", vtctld.TopoImplementation,
		"--topo_global_server_address", vtctld.TopoGlobalAddress,
		"--topo_global_root", vtctld.TopoGlobalRoot,
		"--cell", cell,
		"--service_map", vtctld.ServiceMap,
		"--backup_storage_implementation", vtctld.BackupStorageImplementation,
		vtutils.GetFlagVariantForTestsByVersion("--file-backup-storage-root", vtctldVer), vtctld.FileBackupStorageRoot,
		"--log_dir", vtctld.LogDir,
		"--port", strconv.Itoa(vtctld.Port),
		"--grpc_port", strconv.Itoa(vtctld.GrpcPort),
		"--bind-address", "127.0.0.1",
		"--grpc_bind_address", "127.0.0.1",
	)

	if *isCoverage {
		vtctld.proc.Args = append(vtctld.proc.Args, "--test.coverprofile="+getCoveragePath("vtctld.out"))
	}
	vtctld.proc.Args = append(vtctld.proc.Args, extraArgs...)

	err = os.MkdirAll(vtctld.LogDir, 0755)
	if err != nil {
		log.Errorf("cannot create log directory for vtctld: %v", err)
		return err
	}
	errFile, err := os.Create(path.Join(vtctld.LogDir, "vtctld-stderr.txt"))
	if err != nil {
		log.Errorf("cannot create error log file for vtctld: %v", err)
		return err
	}
	vtctld.proc.Stderr = errFile
	vtctld.ErrorLog = errFile.Name()

	vtctld.proc.Env = append(vtctld.proc.Env, os.Environ()...)
	vtctld.proc.Env = append(vtctld.proc.Env, DefaultVttestEnv)

	log.Infof("Starting vtctld with command: %v", strings.Join(vtctld.proc.Args, " "))

	err = vtctld.proc.Start()
	if err != nil {
		return
	}

	vtctld.exit = make(chan error)
	go func() {
		vtctld.exit <- vtctld.proc.Wait()
		close(vtctld.exit)
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vtctld.IsHealthy() {
			return nil
		}
		select {
		case err := <-vtctld.exit:
			errBytes, ferr := os.ReadFile(vtctld.ErrorLog)
			if ferr == nil {
				log.Errorf("vtctld error log contents:\n%s", string(errBytes))
			} else {
				log.Errorf("Failed to read the vtctld error log file %q: %v", vtctld.ErrorLog, ferr)
			}
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
	defer resp.Body.Close()
	return resp.StatusCode == 200
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
		err := <-vtctld.exit
		vtctld.proc = nil
		return err
	}
}

// VtctldProcessInstance returns a VtctldProcess handle
// configured with the given Config.
// The process must be manually started by calling setup()
func VtctldProcessInstance(httpPort int, grpcPort int, topoPort int, hostname string, tmpDirectory string) *VtctldProcess {
	base := VtProcessInstance("vtctld", "vtctld", topoPort, hostname)
	vtctld := &VtctldProcess{
		VtProcess:                   base,
		ServiceMap:                  "grpc-vtctl,grpc-vtctld",
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
