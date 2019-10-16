package vttest

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
type VttabletProcess struct {
	Name                        string
	Binary                      string
	FileToLogQueries            string
	TabletUID                   int
	TabletPath                  string
	Cell                        string
	Port                        int
	GrpcPort                    int
	PidFile                     string
	Shard                       string
	CommonArg                   VtctlProcess
	LogDir                      string
	TabletHostname              string
	Keyspace                    string
	TabletType                  string
	HealthCheckInterval         int
	BackupStorageImplementation string
	FileBackupStorageRoot       string
	ServiceMap                  string
	VtctldAddress               string
	Directory                   string
	VerifyURL                   string

	proc *exec.Cmd
	exit chan error
}

// Setup starts vtctld process with required arguements
func (vttablet *VttabletProcess) Setup() (err error) {

	vttablet.proc = exec.Command(
		vttablet.Binary,
		"-topo_implementation", vttablet.CommonArg.TopoImplementation,
		"-topo_global_server_address", vttablet.CommonArg.TopoGlobalAddress,
		"-topo_global_root", vttablet.CommonArg.TopoGlobalRoot,
		"-log_queries_to_file", vttablet.FileToLogQueries,
		"-tablet-path", vttablet.TabletPath,
		"-port", fmt.Sprintf("%d", vttablet.Port),
		"-grpc_port", fmt.Sprintf("%d", vttablet.GrpcPort),
		"-pid_file", vttablet.PidFile,
		"-init_shard", vttablet.Shard,
		"-log_dir", vttablet.LogDir,
		"-tablet_hostname", vttablet.TabletHostname,
		"-init_keyspace", vttablet.Keyspace,
		"-init_tablet_type", vttablet.TabletType,
		"-health_check_interval", fmt.Sprintf("%ds", vttablet.HealthCheckInterval),
		"-enable_semi_sync",
		"-enable_replication_reporter",
		"-backup_storage_implementation", vttablet.BackupStorageImplementation,
		"-file_backup_storage_root", vttablet.FileBackupStorageRoot,
		"-restore_from_backup",
		"-service_map", vttablet.ServiceMap,
		"-vtctld_addr", vttablet.VtctldAddress,
	)

	vttablet.proc.Stderr = os.Stderr
	vttablet.proc.Stdout = os.Stdout

	vttablet.proc.Env = append(vttablet.proc.Env, os.Environ()...)

	log.Infof("%v %v", strings.Join(vttablet.proc.Args, " "))

	err = vttablet.proc.Start()
	if err != nil {
		return
	}

	vttablet.exit = make(chan error)
	go func() {
		vttablet.exit <- vttablet.proc.Wait()
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vttablet.IsHealthy() {
			return nil
		}
		select {
		case err := <-vttablet.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vttablet.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", vttablet.Name, <-vttablet.exit)
}

// IsHealthy function checks if vtctld process is up and running
func (vttablet *VttabletProcess) IsHealthy() bool {
	resp, err := http.Get(vttablet.VerifyURL)
	if err != nil {
		return false
	}
	if resp.StatusCode == 200 {
		resultMap := make(map[string]interface{})
		respByte, _ := ioutil.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			panic(err)
		}
		return resultMap["TabletStateName"] == "NOT_SERVING"
	}
	return false
}

// TearDown shutdowns the running vttablet service
func (vttablet *VttabletProcess) TearDown() error {
	if vttablet.proc == nil || vttablet.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	vttablet.proc.Process.Signal(syscall.SIGTERM)

	os.RemoveAll(vttablet.Directory)

	select {
	case err := <-vttablet.exit:
		vttablet.proc = nil
		return err

	case <-time.After(10 * time.Second):
		vttablet.proc.Process.Kill()
		vttablet.proc = nil
		return <-vttablet.exit
	}
}

// VttabletProcessInstance returns a VtctlProcess handle for vtctl process
// configured with the given Config.
// The process must be manually started by calling setup()
func VttabletProcessInstance(Port int, GrpcPort int, TabletUID int, Cell string, Shard string, Hostname string, Keyspace string, VtctldPort int, TabletType string) *VttabletProcess {
	vtctl := VtctlProcessInstance()
	vttablet := &VttabletProcess{
		Name:                        "vttablet",
		Binary:                      "vttablet",
		FileToLogQueries:            path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/tmp/vt_%010d/vttable.pid", TabletUID)),
		Directory:                   path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d", TabletUID)),
		TabletPath:                  fmt.Sprintf("%s-000%d", Cell, TabletUID),
		ServiceMap:                  "grpc-queryservice,grpc-tabletmanager,grpc-updatestream",
		LogDir:                      path.Join(os.Getenv("VTDATAROOT"), "/tmp"),
		Shard:                       Shard,
		TabletHostname:              Hostname,
		Keyspace:                    Keyspace,
		TabletType:                  "replica",
		CommonArg:                   *vtctl,
		HealthCheckInterval:         5,
		BackupStorageImplementation: "file",
		FileBackupStorageRoot:       path.Join(os.Getenv("VTDATAROOT"), "/backups"),
		Port:                        Port,
		GrpcPort:                    GrpcPort,
		PidFile:                     path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/vttable.pid", TabletUID)),
		VtctldAddress:               fmt.Sprintf("http://%s:%d", Hostname, VtctldPort),
	}

	if TabletType != "" {
		vttablet.TabletType = TabletType
	}
	vttablet.VerifyURL = fmt.Sprintf("http://%s:%d/debug/vars", Hostname, Port)

	return vttablet
}
