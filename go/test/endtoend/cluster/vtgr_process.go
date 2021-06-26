package cluster

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// VtgrProcess represents the vtgr process
type VtgrProcess struct {
	VtctlProcess
	LogDir    string
	ExtraArgs []string
	clusters  []string
	config    string
	grPort    int
	proc      *exec.Cmd
	exit      chan error
}

// Start starts vtgr process with required arguements
func (vtgr *VtgrProcess) Start(alias string) (err error) {
	/* minimal command line arguments:
	$ vtgr -topo_implementation etcd2 \
	-topo_global_server_address localhost:2379 \
	-topo_global_root /vitess/global \
	-clusters_to_watch ks/0
	*/
	vtgr.proc = exec.Command(
		vtgr.Binary,
		"-topo_implementation", vtgr.TopoImplementation,
		"-topo_global_server_address", vtgr.TopoGlobalAddress,
		"-topo_global_root", vtgr.TopoGlobalRoot,
		"-tablet_manager_protocol", "grpc",
		"-scan_repair_timeout", "50s",
		"-clusters_to_watch", strings.Join(vtgr.clusters, ","),
	)
	if vtgr.config != "" {
		vtgr.proc.Args = append(vtgr.proc.Args, fmt.Sprintf("-config=%s", vtgr.config))
	}
	if vtgr.grPort != 0 {
		vtgr.proc.Args = append(vtgr.proc.Args, fmt.Sprintf("-gr_port=%d", vtgr.grPort))
	}
	vtgr.proc.Args = append(vtgr.proc.Args, vtgr.ExtraArgs...)
	errFile, _ := os.Create(path.Join(vtgr.LogDir, fmt.Sprintf("vtgr-stderr-%v.txt", alias)))
	vtgr.proc.Stderr = errFile
	vtgr.proc.Env = append(vtgr.proc.Env, os.Environ()...)
	log.Infof("Running vtgr with command: %v", strings.Join(vtgr.proc.Args, " "))
	err = vtgr.proc.Start()
	if err != nil {
		return
	}

	vtgr.exit = make(chan error)
	go func() {
		if vtgr.proc != nil {
			vtgr.exit <- vtgr.proc.Wait()
		}
	}()

	return nil
}

// TearDown shuts down the running vtgr service
func (vtgr *VtgrProcess) TearDown() error {
	if vtgr.proc == nil || vtgr.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	_ = vtgr.proc.Process.Signal(syscall.SIGTERM)

	select {
	case <-vtgr.exit:
		vtgr.proc = nil
		return nil

	case <-time.After(10 * time.Second):
		_ = vtgr.proc.Process.Kill()
		vtgr.proc = nil
		return <-vtgr.exit
	}
}
