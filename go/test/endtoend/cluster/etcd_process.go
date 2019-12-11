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

// EtcdProcess is a generic handle for a running Etcd .
// It can be spawned manually
type EtcdProcess struct {
	Name               string
	Binary             string
	DataDirectory      string
	ListenClientURL    string
	AdvertiseClientURL string
	Port               int
	PeerPort           int
	Host               string
	VerifyURL          string
	PeerURL            string

	proc *exec.Cmd
	exit chan error
}

// Setup spawns a new etcd service and initializes it with the defaults.
// The service is kept running in the background until TearDown() is called.
func (etcd *EtcdProcess) Setup() (err error) {
	etcd.proc = exec.Command(
		etcd.Binary,
		"--name", etcd.Name,
		"--data-dir", etcd.DataDirectory,
		"--listen-client-urls", etcd.ListenClientURL,
		"--advertise-client-urls", etcd.AdvertiseClientURL,
		"--initial-advertise-peer-urls", etcd.PeerURL,
		"--listen-peer-urls", etcd.PeerURL,
		"--initial-cluster", fmt.Sprintf("%s=%s", etcd.Name, etcd.PeerURL),
	)

	errFile, _ := os.Create(path.Join(etcd.DataDirectory, "etcd-stderr.txt"))
	etcd.proc.Stderr = errFile

	etcd.proc.Env = append(etcd.proc.Env, os.Environ()...)

	log.Infof("%v %v", strings.Join(etcd.proc.Args, " "))
	println("Starting etcd with args " + strings.Join(etcd.proc.Args, " "))
	err = etcd.proc.Start()
	if err != nil {
		return
	}

	etcd.exit = make(chan error)
	go func() {
		etcd.exit <- etcd.proc.Wait()
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if etcd.IsHealthy() {
			return
		}
		select {
		case err := <-etcd.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", etcd.Binary, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", etcd.Binary, <-etcd.exit)
}

// TearDown shutdowns the running mysqld service
func (etcd *EtcdProcess) TearDown(Cell string, originalVtRoot string, currentRoot string, keepdata bool) error {
	if etcd.proc == nil || etcd.exit == nil {
		return nil
	}

	etcd.removeTopoDirectories(Cell)

	// Attempt graceful shutdown with SIGTERM first
	_ = etcd.proc.Process.Signal(syscall.SIGTERM)
	if !*keepData {
		_ = os.RemoveAll(etcd.DataDirectory)
		_ = os.RemoveAll(currentRoot)
	}
	_ = os.Setenv("VTDATAROOT", originalVtRoot)
	select {
	case <-etcd.exit:
		etcd.proc = nil
		return nil

	case <-time.After(10 * time.Second):
		etcd.proc.Process.Kill()
		etcd.proc = nil
		return <-etcd.exit
	}

}

// IsHealthy function checks if etcd server is up and running
func (etcd *EtcdProcess) IsHealthy() bool {
	resp, err := http.Get(etcd.VerifyURL)
	if err != nil {
		return false
	}
	if resp.StatusCode == 200 {
		return true
	}
	return false
}

func (etcd *EtcdProcess) removeTopoDirectories(Cell string) {
	_ = etcd.ManageTopoDir("rmdir", "/vitess/global")
	_ = etcd.ManageTopoDir("rmdir", "/vitess/"+Cell)
}

// ManageTopoDir creates global and zone in etcd2
func (etcd *EtcdProcess) ManageTopoDir(command string, directory string) (err error) {
	url := etcd.VerifyURL + directory
	payload := strings.NewReader(`{"dir":"true"}`)
	if command == "mkdir" {
		req, _ := http.NewRequest("PUT", url, payload)
		req.Header.Add("content-type", "application/json")
		_, err = http.DefaultClient.Do(req)
		return err
	} else if command == "rmdir" {
		req, _ := http.NewRequest("DELETE", url+"?dir=true", payload)
		_, err = http.DefaultClient.Do(req)
		return err
	} else {
		return nil
	}
}

// EtcdProcessInstance returns a EtcdProcess handle for a etcd sevice,
// configured with the given Config.
// The process must be manually started by calling setup()
func EtcdProcessInstance(port int, peerPort int, hostname string, name string) *EtcdProcess {
	etcd := &EtcdProcess{
		Name:     name,
		Binary:   "etcd",
		Port:     port,
		Host:     hostname,
		PeerPort: peerPort,
	}

	etcd.AdvertiseClientURL = fmt.Sprintf("http://%s:%d", etcd.Host, etcd.Port)
	etcd.ListenClientURL = fmt.Sprintf("http://%s:%d", etcd.Host, etcd.Port)
	etcd.DataDirectory = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("%s_%d", "etcd", port))
	etcd.VerifyURL = fmt.Sprintf("http://%s:%d/v2/keys", etcd.Host, etcd.Port)
	etcd.PeerURL = fmt.Sprintf("http://%s:%d", hostname, peerPort)
	return etcd
}
