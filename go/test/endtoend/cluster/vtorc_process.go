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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// VTOrcProcess is a test struct for running
// vtorc as a separate process for testing
type VTOrcProcess struct {
	VtctlProcess
	Port       int
	LogDir     string
	ExtraArgs  []string
	ConfigPath string
	Config     VTOrcConfiguration
	WebPort    int
	proc       *exec.Cmd
	exit       chan error
}

type VTOrcConfiguration struct {
	Debug                                 bool
	ListenAddress                         string
	MySQLTopologyUser                     string
	MySQLTopologyPassword                 string
	MySQLReplicaUser                      string
	MySQLReplicaPassword                  string
	RecoveryPeriodBlockSeconds            int
	InstancePollSeconds                   int
	PreventCrossDataCenterPrimaryFailover bool   `json:",omitempty"`
	LockShardTimeoutSeconds               int    `json:",omitempty"`
	ReplicationLagQuery                   string `json:",omitempty"`
	FailPrimaryPromotionOnLagMinutes      int    `json:",omitempty"`
}

// ToJSONString will marshal this configuration as JSON
func (config *VTOrcConfiguration) ToJSONString() string {
	b, _ := json.MarshalIndent(config, "", "\t")
	return string(b)
}

func (config *VTOrcConfiguration) AddDefaults(webPort int) {
	config.Debug = true
	config.MySQLTopologyUser = "orc_client_user"
	config.MySQLTopologyPassword = "orc_client_user_password"
	config.MySQLReplicaUser = "vt_repl"
	config.MySQLReplicaPassword = ""
	if config.RecoveryPeriodBlockSeconds == 0 {
		config.RecoveryPeriodBlockSeconds = 1
	}
	config.InstancePollSeconds = 1
	config.ListenAddress = fmt.Sprintf(":%d", webPort)
}

// Setup starts orc process with required arguements
func (orc *VTOrcProcess) Setup() (err error) {

	// create the configuration file
	timeNow := time.Now().UnixNano()
	configFile, _ := os.Create(path.Join(orc.LogDir, fmt.Sprintf("orc-config-%d.json", timeNow)))
	orc.ConfigPath = configFile.Name()

	// Add the default configurations and print them out
	orc.Config.AddDefaults(orc.WebPort)
	log.Errorf("configuration - %v", orc.Config.ToJSONString())
	_, err = configFile.WriteString(orc.Config.ToJSONString())
	if err != nil {
		return err
	}
	err = configFile.Close()
	if err != nil {
		return err
	}

	/* minimal command line arguments:
	$ vtorc --topo_implementation etcd2 --topo_global_server_address localhost:2379 --topo_global_root /vitess/global
	--config config/vtorc/default.json --alsologtostderr
	*/
	orc.proc = exec.Command(
		orc.Binary,
		"--topo_implementation", orc.TopoImplementation,
		"--topo_global_server_address", orc.TopoGlobalAddress,
		"--topo_global_root", orc.TopoGlobalRoot,
		"--config", orc.ConfigPath,
		"--port", fmt.Sprintf("%d", orc.Port),
		"--orc_web_dir", path.Join(os.Getenv("VTROOT"), "web", "vtorc"),
	)
	if *isCoverage {
		orc.proc.Args = append(orc.proc.Args, "--test.coverprofile="+getCoveragePath("orc.out"))
	}

	orc.proc.Args = append(orc.proc.Args, orc.ExtraArgs...)
	orc.proc.Args = append(orc.proc.Args, "--alsologtostderr")

	errFile, _ := os.Create(path.Join(orc.LogDir, fmt.Sprintf("orc-stderr-%d.txt", timeNow)))
	orc.proc.Stderr = errFile

	orc.proc.Env = append(orc.proc.Env, os.Environ()...)

	log.Infof("Running vtorc with command: %v", strings.Join(orc.proc.Args, " "))

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
func (orc *VTOrcProcess) TearDown() error {
	if orc.proc == nil || orc.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	_ = orc.proc.Process.Signal(syscall.SIGTERM)

	select {
	case <-orc.exit:
		orc.proc = nil
		return nil

	case <-time.After(30 * time.Second):
		_ = orc.proc.Process.Kill()
		orc.proc = nil
		return <-orc.exit
	}
}

// GetVars gets the variables exported on the /debug/vars page of VTOrc
func (orc *VTOrcProcess) GetVars() map[string]any {
	varsURL := fmt.Sprintf("http://localhost:%d/debug/vars", orc.Port)
	resp, err := http.Get(varsURL)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		resultMap := make(map[string]any)
		respByte, _ := io.ReadAll(resp.Body)
		err := json.Unmarshal(respByte, &resultMap)
		if err != nil {
			return nil
		}
		return resultMap
	}
	return nil
}
