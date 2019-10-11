/*
Copyright 2017 GitHub Inc.

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

package vttest

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
	Host               string
	VerifyURL          string

	proc *exec.Cmd
	exit chan error
}

// Setup spawns a new etcd service and initializes it with the defaults.
// The service is kept running in the background until TearDown() is called.
func (etcd *EtcdProcess) Setup() (err error) {
	etcd.proc = exec.Command(
		etcd.Binary,
		"--data-dir", etcd.DataDirectory,
		"--listen-client-urls", etcd.ListenClientURL,
		"--advertise-client-urls", etcd.AdvertiseClientURL,
	)

	etcd.proc.Stderr = os.Stderr
	etcd.proc.Stdout = os.Stdout

	etcd.proc.Env = append(etcd.proc.Env, os.Environ()...)

	log.Infof("%v %v", strings.Join(etcd.proc.Args, " "))
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
			return nil
		}
		select {
		case err := <-etcd.exit:
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", etcd.Name, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", etcd.Name, <-etcd.exit)
}

// TearDown shutdowns the running mysqld service
func (etcd *EtcdProcess) TearDown() error {
	if etcd.proc == nil || etcd.exit == nil {
		return nil
	}

	// Attempt graceful shutdown with SIGTERM first
	etcd.proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-etcd.exit:
		etcd.proc = nil
		return err

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

// EtcdProcessInstance returns a EtcdProcess handle for a etcd sevice,
// configured with the given Config.
// The process must be manually started by calling setup()
func EtcdProcessInstance() *EtcdProcess {
	etcd := &EtcdProcess{
		Name:   "etcd",
		Binary: "etcd",
		Port:   2379,
		Host:   "localhost",
	}

	etcd.AdvertiseClientURL = fmt.Sprintf("http://%s:%d", etcd.Host, etcd.Port)
	etcd.ListenClientURL = fmt.Sprintf("http://%s:%d", etcd.Host, etcd.Port)
	etcd.DataDirectory = path.Join(os.Getenv("VTDATAROOT"), "etcd")
	etcd.VerifyURL = fmt.Sprintf("http://%s:%d/v2/keys", etcd.Host, etcd.Port)
	return etcd
}
