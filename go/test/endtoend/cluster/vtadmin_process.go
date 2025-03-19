/*
Copyright 2024 The Vitess Authors.

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
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

// VtAdminProcess is a test struct for running
// vtorc as a separate process for testing
type VtAdminProcess struct {
	Binary string
	Port   int
	LogDir string
	// ClusterID is the cluster identifier used when passing the --cluster
	// flag to vtadmin. If unset, defaults to "local".
	ClusterID      string
	ExtraArgs      []string
	VtGateGrpcPort int
	VtGateWebPort  int
	VtctldGrpcPort int
	VtctldWebPort  int
	proc           *exec.Cmd
	exit           chan error
}

// Setup starts orc process with required arguements
func (vp *VtAdminProcess) Setup() (err error) {
	// create the configuration file
	timeNow := time.Now().UnixNano()
	err = os.MkdirAll(vp.LogDir, 0755)
	if err != nil {
		log.Errorf("cannot create log directory for vtadmin: %v", err)
		return err
	}
	rbacFile, err := vp.CreateAndWriteFile("rbac", `rules:
  - resource: "*"
    actions: ["*"]
    subjects: ["*"]
    clusters: ["*"]
`, "yaml")
	if err != nil {
		return err
	}

	discoveryFile, err := vp.CreateAndWriteFile("discovery", fmt.Sprintf(`
{
    "vtctlds": [
        {
            "host": {
                "fqdn": "localhost:%d",
                "hostname": "localhost:%d"
            }
        }
    ],
    "vtgates": [
        {
            "host": {
                "fqdn": "localhost:%d",
                "hostname": "localhost:%d"
            }
        }
    ]
}
`, vp.VtctldWebPort, vp.VtctldGrpcPort, vp.VtGateWebPort, vp.VtGateGrpcPort), "json")
	if err != nil {
		return err
	}

	// Only allow override if explicitly set by tests; default cluster ID is "local".
	clusterID := "local"
	if vp.ClusterID != "" {
		clusterID = vp.ClusterID
	}
	vp.proc = exec.Command(
		vp.Binary,
		"--addr", vp.Address(),
		"--http-tablet-url-tmpl", `"http://{{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }}"`,
		"--tracer", `"opentracing-jaeger"`,
		"--grpc-tracing",
		"--http-tracing",
		"--logtostderr",
		"--alsologtostderr",
		"--rbac",
		"--rbac-config", rbacFile,
		"--cluster", fmt.Sprintf(`id=%s,name=%s,discovery=staticfile,discovery-staticfile-path=%s,tablet-fqdn-tmpl=http://{{ .Tablet.Hostname }}:15{{ .Tablet.Alias.Uid }},schema-cache-default-expiration=1m`,
			clusterID,
			clusterID,
			discoveryFile),
	)

	if *isCoverage {
		vp.proc.Args = append(vp.proc.Args, "--test.coverprofile="+getCoveragePath("vp.out"))
	}

	vp.proc.Args = append(vp.proc.Args, vp.ExtraArgs...)
	vp.proc.Args = append(vp.proc.Args, "--alsologtostderr")

	logFile := fmt.Sprintf("vtadmin-stderr-%d.txt", timeNow)
	errFile, err := os.Create(path.Join(vp.LogDir, logFile))
	if err != nil {
		log.Errorf("cannot create error log file for vtadmin: %v", err)
		return err
	}
	vp.proc.Stderr = errFile

	vp.proc.Env = append(vp.proc.Env, os.Environ()...)
	vp.proc.Env = append(vp.proc.Env, DefaultVttestEnv)

	log.Infof("Running vtadmin with command: %v", strings.Join(vp.proc.Args, " "))

	err = vp.proc.Start()
	if err != nil {
		return
	}

	vp.exit = make(chan error)
	go func() {
		if vp.proc != nil {
			exitErr := vp.proc.Wait()
			if exitErr != nil {
				log.Errorf("vtadmin process exited with error: %v", exitErr)
				data, _ := os.ReadFile(logFile)
				log.Errorf("vtadmin stderr - %s", string(data))
			}
			vp.exit <- exitErr
			close(vp.exit)
		}
	}()

	return nil
}

// CreateAndWriteFile creates a file and writes the content to it.
func (vp *VtAdminProcess) CreateAndWriteFile(prefix string, content string, extension string) (string, error) {
	timeNow := time.Now().UnixNano()
	file, err := os.Create(path.Join(vp.LogDir, fmt.Sprintf("%s-%d.%s", prefix, timeNow, extension)))
	if err != nil {
		log.Errorf("cannot create file for vtadmin: %v", err)
		return "", err
	}

	_, err = file.WriteString(content)
	if err != nil {
		return "", err
	}
	fileName := file.Name()
	err = file.Close()
	if err != nil {
		return "", err
	}
	return fileName, nil
}

// Address returns the address of the running vtadmin service.
func (vp *VtAdminProcess) Address() string {
	return fmt.Sprintf("localhost:%d", vp.Port)
}

// TearDown shuts down the running vtorc service
func (vp *VtAdminProcess) TearDown() error {
	if vp.proc == nil || vp.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	_ = vp.proc.Process.Signal(syscall.SIGTERM)

	select {
	case <-vp.exit:
		vp.proc = nil
		return nil

	case <-time.After(30 * time.Second):
		_ = vp.proc.Process.Kill()
		err := <-vp.exit
		vp.proc = nil
		return err
	}
}

// MakeAPICall makes an API call on the given endpoint of VTOrc
func (vp *VtAdminProcess) MakeAPICall(endpoint string) (status int, response string, err error) {
	url := fmt.Sprintf("http://%s/%s", vp.Address(), endpoint)
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
func (vp *VtAdminProcess) MakeAPICallRetry(t *testing.T, url string) string {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			require.FailNow(t, "timed out waiting for api to work")
			return ""
		default:
			status, resp, err := vp.MakeAPICall(url)
			if err == nil && status == 200 {
				return resp
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
