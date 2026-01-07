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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/utils"
)

// VTOrcProcess is a test struct for running
// vtorc as a separate process for testing
type VTOrcProcess struct {
	VtProcess
	Port        int
	Cell        string
	LogDir      string
	LogFileName string
	ExtraArgs   []string
	ConfigPath  string
	Config      VTOrcConfiguration
	NoOverride  bool
	proc        *exec.Cmd
	exit        chan error
}

type VTOrcConfiguration struct {
	InstancePollTime                     string `json:"instance-poll-time,omitempty"`
	SnapshotTopologyInterval             string `json:"snapshot-topology-interval,omitempty"`
	PreventCrossCellFailover             bool   `json:"prevent-cross-cell-failover,omitempty"`
	ReasonableReplicationLag             string `json:"reasonable-replication-lag,omitempty"`
	AuditToBackend                       bool   `json:"audit-to-backend,omitempty"`
	AuditToSyslog                        bool   `json:"audit-to-syslog,omitempty"`
	AuditPurgeDuration                   string `json:"audit-purge-duration,omitempty"`
	WaitReplicasTimeout                  string `json:"wait-replicas-timeout,omitempty"`
	TolerableReplicationLag              string `json:"tolerable-replication-lag,omitempty"`
	TopoInformationRefreshDuration       string `json:"topo-information-refresh-duration,omitempty"`
	RecoveryPollDuration                 string `json:"recovery-poll-duration,omitempty"`
	AllowEmergencyReparent               string `json:"allow-emergency-reparent,omitempty"`
	ChangeTabletsWithErrantGtidToDrained bool   `json:"change-tablets-with-errant-gtid-to-drained,omitempty"`
	LockShardTimeoutSeconds              int    `json:",omitempty"`
	ReplicationLagQuery                  string `json:",omitempty"`
	FailPrimaryPromotionOnLagMinutes     int    `json:",omitempty"`
}

// ToJSONString will marshal this configuration as JSON
func (config *VTOrcConfiguration) ToJSONString() string {
	b, _ := json.MarshalIndent(config, "", "\t")
	return string(b)
}

func (config *VTOrcConfiguration) addValuesToCheckOverride() {
	config.InstancePollTime = "10h"
}

func (orc *VTOrcProcess) RewriteConfiguration() error {
	return os.WriteFile(orc.ConfigPath, []byte(orc.Config.ToJSONString()), 0644)
}

// Setup starts orc process with required arguements
func (orc *VTOrcProcess) Setup() (err error) {
	// validate cell
	if orc.Cell == "" {
		return errors.New("vtorc cell cannot be empty")
	}

	// create the configuration file
	timeNow := time.Now().UnixNano()
	err = os.MkdirAll(orc.LogDir, 0755)
	if err != nil {
		log.Errorf("cannot create log directory for vtorc: %v", err)
		return err
	}
	configFile, err := os.Create(path.Join(orc.LogDir, fmt.Sprintf("orc-config-%d.json", timeNow)))
	if err != nil {
		log.Errorf("cannot create config file for vtorc: %v", err)
		return err
	}
	orc.ConfigPath = configFile.Name()

	// Add the default configurations and print them out
	if !orc.NoOverride {
		orc.Config.addValuesToCheckOverride()
	}
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
	$ vtorc --topo-implementation etcd2 --topo-global-server-address localhost:2379 --topo-global-root /vitess/global
	--config config/vtorc/default.json --alsologtostderr
	*/
	flags := map[string]string{
		"--cell":                       orc.Cell,
		"--topo-implementation":        orc.TopoImplementation,
		"--topo-global-server-address": orc.TopoGlobalAddress,
		"--topo-global-root":           orc.TopoGlobalRoot,
		"--config-file":                orc.ConfigPath,
		"--port":                       strconv.Itoa(orc.Port),
		"--bind-address":               "127.0.0.1",
	}

	utils.SetFlagVariantsForTests(flags, "--topo-implementation", orc.TopoImplementation)
	utils.SetFlagVariantsForTests(flags, "--topo-global-server-address", orc.TopoGlobalAddress)
	utils.SetFlagVariantsForTests(flags, "--topo-global-root", orc.TopoGlobalRoot)

	orc.proc = exec.Command(orc.Binary)
	for flag, value := range flags {
		orc.proc.Args = append(orc.proc.Args, flag, value)
	}

	if !orc.NoOverride {
		orc.proc.Args = append(orc.proc.Args,
			// This parameter is overriden from the config file. This verifies that we indeed use the flag value over the config file.
			"--instance-poll-time", "1s",
			// Faster topo information refresh speeds up the tests. This doesn't add any significant load either.
			"--topo-information-refresh-duration", "3s",
		)
	}

	if *isCoverage {
		orc.proc.Args = append(orc.proc.Args, "--test.coverprofile="+getCoveragePath("orc.out"))
	}

	orc.proc.Args = append(orc.proc.Args, orc.ExtraArgs...)
	orc.proc.Args = append(orc.proc.Args, "--alsologtostderr")

	if orc.LogFileName == "" {
		orc.LogFileName = fmt.Sprintf("vtorc-stderr-%d.txt", timeNow)
	}
	errFile, err := os.Create(path.Join(orc.LogDir, orc.LogFileName))
	if err != nil {
		log.Errorf("cannot create error log file for vtorc: %v", err)
		return err
	}
	orc.proc.Stderr = errFile

	orc.proc.Env = append(orc.proc.Env, os.Environ()...)
	orc.proc.Env = append(orc.proc.Env, DefaultVttestEnv)

	log.Infof("Running vtorc with command: %v", strings.Join(orc.proc.Args, " "))

	err = orc.proc.Start()
	if err != nil {
		return
	}

	orc.exit = make(chan error)
	go func() {
		if orc.proc != nil {
			orc.exit <- orc.proc.Wait()
			close(orc.exit)
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
		err := <-orc.exit
		orc.proc = nil
		return err
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

// GetMetrics gets the metrics exported on the /metrics page of VTOrc
func (orc *VTOrcProcess) GetMetrics() string {
	varsURL := fmt.Sprintf("http://localhost:%d/metrics", orc.Port)
	resp, err := http.Get(varsURL)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		respByte, _ := io.ReadAll(resp.Body)
		return string(respByte)
	}
	return ""
}

// MakeAPICall makes an API call on the given endpoint of VTOrc
func (orc *VTOrcProcess) MakeAPICall(endpoint string) (status int, response string, err error) {
	url := fmt.Sprintf("http://localhost:%d/%s", orc.Port, endpoint)
	resp, err := http.Get(url)
	if err != nil {
		if resp != nil {
			status = resp.StatusCode
		}
		return status, "", err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	respByte, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(respByte), err
}

// MakeAPICallRetry is used to make an API call and retries until success
func (orc *VTOrcProcess) MakeAPICallRetry(t *testing.T, url string) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for api to work")
			return
		default:
			status, _, err := orc.MakeAPICall(url)
			if err == nil && status == 200 {
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}

// DisableGlobalRecoveries stops VTOrc from running any recoveries
func (orc *VTOrcProcess) DisableGlobalRecoveries(t *testing.T) {
	orc.MakeAPICallRetry(t, "/api/disable-global-recoveries")
}

// EnableGlobalRecoveries allows VTOrc to run any recoveries
func (orc *VTOrcProcess) EnableGlobalRecoveries(t *testing.T) {
	orc.MakeAPICallRetry(t, "/api/enable-global-recoveries")
}
