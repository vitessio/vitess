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

// VtgateProcess is a generic handle for a running vtgate .
// It can be spawned manually
type VtgateProcess struct {
	Name                  string
	Binary                string
	CommonArg             VtctlProcess
	LogDir                string
	FileToLogQueries      string
	Port                  int
	GrpcPort              int
	MySqlServerPort       int
	MySqlServerSocketPath string
	Cell                  string
	CellsToWatch          string
	TabletTypesToWait     string
	GatewayImplementation string
	ServiceMap            string
	PidFile               string
	MySqlAuthServerImpl   string
	Directory             string
	VerifyURL             string

	proc *exec.Cmd
	exit chan error
}

// Setup starts Vtgate process with required arguements
func (vtgate *VtgateProcess) Setup() (err error) {

	vtgate.proc = exec.Command(
		vtgate.Binary,
		"-topo_implementation", vtgate.CommonArg.TopoImplementation,
		"-topo_global_server_address", vtgate.CommonArg.TopoGlobalAddress,
		"-topo_global_root", vtgate.CommonArg.TopoGlobalRoot,
		"-log_dir", vtgate.LogDir,
		"-log_queries_to_file", vtgate.FileToLogQueries,
		"-port", fmt.Sprintf("%d", vtgate.Port),
		"-grpc_port", fmt.Sprintf("%d", vtgate.GrpcPort),
		"-mysql_server_port", fmt.Sprintf("%d", vtgate.MySqlServerPort),
		"-mysql_server_socket_path", vtgate.MySqlServerSocketPath,
		"-cell", vtgate.Cell,
		"-cells_to_watch", vtgate.CellsToWatch,
		"-tablet_types_to_wait", vtgate.TabletTypesToWait,
		"-gateway_implementation", vtgate.GatewayImplementation,
		"-service_map", vtgate.ServiceMap,
		"-mysql_auth_server_impl", vtgate.MySqlAuthServerImpl,
		"-pid_file", vtgate.PidFile,
	)

	vtgate.proc.Stderr = os.Stderr
	vtgate.proc.Stdout = os.Stdout

	vtgate.proc.Env = append(vtgate.proc.Env, os.Environ()...)

	log.Infof("%v %v", strings.Join(vtgate.proc.Args, " "))

	err = vtgate.proc.Start()
	if err != nil {
		return
	}

	vtgate.exit = make(chan error)
	go func() {
		vtgate.exit <- vtgate.proc.Wait()
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if vtgate.IsHealthy() {
			return nil
		}
		select {
		case err := <-vtgate.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", vtgate.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", vtgate.Name, <-vtgate.exit)
}

// IsHealthy function checks if vtgate process is up and running
func (vtgate *VtgateProcess) IsHealthy() bool {
	resp, err := http.Get(vtgate.VerifyURL)
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
		//for key, value := range resultMap {
		//	println("VTGate API Response: Key = " + key + ", value = " + fmt.Sprintf("%v", value))
		//}
		//println(string(respByte))
		//return resultMap["TabletStateName"] == "NOT_SERVING"
		return true
	}
	return false
}

// TearDown shutdowns the running vttablet service
func (vtgate *VtgateProcess) TearDown() error {
	if vtgate.proc == nil || vtgate.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	vtgate.proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-vtgate.exit:
		vtgate.proc = nil
		return err

	case <-time.After(10 * time.Second):
		vtgate.proc.Process.Kill()
		vtgate.proc = nil
		return <-vtgate.exit
	}
}

// VtgateProcessInstance returns a Vtgate handle for vtgate process
// configured with the given Config.
// The process must be manually started by calling setup()
func VtgateProcessInstance(Port int, GrpcPort int, MySqlServerPort int, Cell string, CellsToWatch string, Hostname string, TabletTypesToWait string) *VtgateProcess {
	vtctl := VtctlProcessInstance()
	vtgate := &VtgateProcess{
		Name:                  "vtgate",
		Binary:                "vtgate",
		FileToLogQueries:      path.Join(os.Getenv("VTDATAROOT"), "/tmp/vtgate_querylog.txt"),
		Directory:             os.Getenv("VTDATAROOT"),
		ServiceMap:            "grpc-vtgateservice",
		LogDir:                path.Join(os.Getenv("VTDATAROOT"), "/tmp"),
		Port:                  Port,
		GrpcPort:              GrpcPort,
		MySqlServerPort:       MySqlServerPort,
		MySqlServerSocketPath: "/tmp/mysql.sock",
		Cell:                  Cell,
		CellsToWatch:          CellsToWatch,
		TabletTypesToWait:     TabletTypesToWait,
		GatewayImplementation: "discoverygateway",
		CommonArg:             *vtctl,
		PidFile:               path.Join(os.Getenv("VTDATAROOT"), "/tmp/vtgate.pid"),
		MySqlAuthServerImpl:   "none",
	}

	vtgate.VerifyURL = fmt.Sprintf("http://%s:%d/debug/vars", Hostname, Port)

	return vtgate
}
