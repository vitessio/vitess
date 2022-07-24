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
	LogDir     string
	ExtraArgs  []string
	ConfigPath string
	Config     VtorcConfiguration
	WebPort    int
	proc       *exec.Cmd
	exit       chan error
}

type VtorcConfiguration struct {
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
func (config *VtorcConfiguration) ToJSONString() string {
	b, _ := json.MarshalIndent(config, "", "\t")
	return string(b)
}

func (config *VtorcConfiguration) AddDefaults(webPort int) {
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
func (orc *VtorcProcess) Setup() (err error) {

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
	$ vtorc -topo_implementation etcd2 -topo_global_server_address localhost:2379 -topo_global_root /vitess/global
	-config config/orchestrator/default.json -alsologtostderr http
	*/
	orc.proc = exec.Command(
		orc.Binary,
		"--topo_implementation", orc.TopoImplementation,
		"--topo_global_server_address", orc.TopoGlobalAddress,
		"--topo_global_root", orc.TopoGlobalRoot,
		"--config", orc.ConfigPath,
		"--orc_web_dir", path.Join(os.Getenv("VTROOT"), "web", "orchestrator"),
	)
	if *isCoverage {
		orc.proc.Args = append(orc.proc.Args, "--test.coverprofile="+getCoveragePath("orc.out"))
	}

	orc.proc.Args = append(orc.proc.Args, orc.ExtraArgs...)
	orc.proc.Args = append(orc.proc.Args, "--alsologtostderr", "http")

	errFile, _ := os.Create(path.Join(orc.LogDir, fmt.Sprintf("orc-stderr-%d.txt", timeNow)))
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

	case <-time.After(30 * time.Second):
		_ = orc.proc.Process.Kill()
		orc.proc = nil
		return <-orc.exit
	}
}
