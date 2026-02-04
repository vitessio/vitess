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
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/vt/log"
	vtopo "vitess.io/vitess/go/vt/topo"

	// Register topo server implementations
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	_ "vitess.io/vitess/go/vt/topo/zk2topo"
)

// TopoProcess is a generic handle for a running Topo service .
// It can be spawned manually
type TopoProcess struct {
	Name               string
	Binary             string
	DataDirectory      string
	LogDirectory       string
	ErrorLog           string
	ListenClientURL    string
	AdvertiseClientURL string
	Port               int
	Host               string
	VerifyURL          string
	PeerURL            string
	ZKPorts            string
	Client             any
	Server             *vtopo.Server

	proc *exec.Cmd
	exit chan error
}

// Setup starts a new topo service
func (topo *TopoProcess) Setup(topoFlavor string, cluster *LocalProcessCluster) (err error) {
	switch topoFlavor {
	case "zk2":
		err = topo.SetupZookeeper(cluster)
	default:
		// Override any inherited ETCDCTL_API env value to
		// ensure that we use the v3 API and storage.
		os.Setenv("ETCDCTL_API", "3")
		err = topo.SetupEtcd()
	}

	if err != nil {
		return
	}

	topo.Server, err = vtopo.OpenServer(topoFlavor, net.JoinHostPort(topo.Host, strconv.Itoa(topo.Port)), TopoGlobalRoot(topoFlavor))
	return
}

// SetupEtcd spawns a new etcd service and initializes it with the defaults.
// The service is kept running in the background until TearDown() is called.
func (topo *TopoProcess) SetupEtcd() (err error) {
	topo.proc = exec.Command(
		topo.Binary,
		"--name", topo.Name,
		"--data-dir", topo.DataDirectory,
		"--listen-client-urls", topo.ListenClientURL,
		"--advertise-client-urls", topo.AdvertiseClientURL,
		"--initial-advertise-peer-urls", topo.PeerURL,
		"--listen-peer-urls", topo.PeerURL,
		"--initial-cluster", fmt.Sprintf("%s=%s", topo.Name, topo.PeerURL),
	)

	err = createDirectory(topo.DataDirectory, 0o700)
	if err != nil && !os.IsExist(err) {
		return err
	}
	errFile, err := os.Create(path.Join(topo.DataDirectory, "topo-stderr.txt"))
	if err != nil {
		return err
	}

	topo.proc.Stderr = errFile
	topo.ErrorLog = errFile.Name()

	topo.proc.Env = append(topo.proc.Env, os.Environ()...)
	topo.proc.Env = append(topo.proc.Env, DefaultVttestEnv)

	log.Infof("Starting etcd with command: %v", strings.Join(topo.proc.Args, " "))

	err = topo.proc.Start()
	if err != nil {
		return
	}

	topo.exit = make(chan error)
	go func() {
		topo.exit <- topo.proc.Wait()
		close(topo.exit)
	}()

	timeout := time.Now().Add(60 * time.Second)
	for time.Now().Before(timeout) {
		if topo.IsHealthy() {
			cli, cerr := clientv3.New(clientv3.Config{
				Endpoints:   []string{net.JoinHostPort(topo.Host, strconv.Itoa(topo.Port))},
				DialTimeout: 5 * time.Second,
			})
			if cerr != nil {
				return cerr
			}
			topo.Client = cli
			return
		}
		select {
		case err := <-topo.exit:
			errBytes, ferr := os.ReadFile(topo.ErrorLog)
			if ferr == nil {
				log.Errorf("%s error log contents:\n%s", topo.Binary, string(errBytes))
			} else {
				log.Errorf("Failed to read the %s error log file %q: %v", topo.Binary, topo.ErrorLog, ferr)
			}
			return fmt.Errorf("process '%s' exited prematurely (err: %s)", topo.Binary, err)
		default:
			time.Sleep(300 * time.Millisecond)
		}
	}

	return fmt.Errorf("process '%s' timed out after 60s (err: %s)", topo.Binary, <-topo.exit)
}

// SetupZookeeper spawns a new zookeeper topo service and initializes it with the defaults.
// The service is kept running in the background until TearDown() is called.
func (topo *TopoProcess) SetupZookeeper(cluster *LocalProcessCluster) error {
	host, err := os.Hostname()
	if err != nil {
		return err
	}

	topo.ZKPorts = fmt.Sprintf("%d:%d:%d", cluster.GetAndReservePort(), cluster.GetAndReservePort(), topo.Port)

	topo.proc = exec.Command(
		topo.Binary,
		"--log_dir", topo.LogDirectory,
		"--zk.cfg", fmt.Sprintf("1@%v:%s", host, topo.ZKPorts),
		"init",
	)

	err = os.MkdirAll(topo.LogDirectory, 0o755)
	if err != nil {
		log.Errorf("Failed to create log directory for zookeeper: %v", err)
		return err
	}
	errFile, err := os.Create(path.Join(topo.LogDirectory, "topo-stderr.txt"))
	if err != nil {
		log.Errorf("Failed to create file for zookeeper stderr: %v", err)
		return err
	}
	topo.proc.Stderr = errFile
	topo.proc.Env = append(topo.proc.Env, os.Environ()...)

	log.Infof("Starting zookeeper with args %v", strings.Join(topo.proc.Args, " "))
	return topo.proc.Run()
}

// TearDown shutdowns the running topo service.
func (topo *TopoProcess) TearDown(Cell string, originalVtRoot string, currentRoot string, keepdata bool, topoFlavor string) error {
	if topo.Server != nil {
		topo.Server.Close()
		topo.Server = nil
	}

	if topo.Client != nil {
		switch cli := topo.Client.(type) {
		case *clientv3.Client:
			_ = cli.Close()
		default:
			log.Errorf("Unknown topo client type %T", cli)
		}
	}

	if topoFlavor == "zk2" {
		cmd := "shutdown"
		if keepdata {
			cmd = "teardown"
		}
		topo.proc = exec.Command(
			topo.Binary,
			"--log_dir", topo.LogDirectory,
			"--zk.cfg", fmt.Sprintf("1@%v:%s", topo.Host, topo.ZKPorts),
			cmd,
		)

		err := topo.proc.Run()
		if err != nil {
			return err
		}
	} else {
		if topo.proc == nil || topo.exit == nil {
			return nil
		}

		if !*keepData && !keepdata {
			topo.removeTopoDirectories(Cell)
		}

		// Attempt graceful shutdown with SIGTERM first
		_ = topo.proc.Process.Signal(syscall.SIGTERM)

		if !*keepData && !keepdata {
			_ = os.RemoveAll(topo.DataDirectory)
			_ = os.RemoveAll(currentRoot)
			_ = os.Setenv("VTDATAROOT", originalVtRoot)
		}

		select {
		case <-topo.exit:
			topo.proc = nil
			return nil

		case <-time.After(10 * time.Second):
			topo.proc.Process.Kill()
			err := <-topo.exit
			topo.proc = nil
			return err
		}
	}

	return nil
}

// IsHealthy function checks if topo server is up and running
func (topo *TopoProcess) IsHealthy() bool {
	resp, err := http.Get(topo.VerifyURL)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func (topo *TopoProcess) removeTopoDirectories(Cell string) {
	if err := topo.ManageTopoDir("rmdir", "/vitess/global"); err != nil {
		log.Errorf("Failed to remove global topo directory: %v", err)
	}
	if err := topo.ManageTopoDir("rmdir", "/vitess/"+Cell); err != nil {
		log.Errorf("Failed to remove local topo directory: %v", err)
	}
}

// ManageTopoDir creates global and zone in etcd2
func (topo *TopoProcess) ManageTopoDir(command string, directory string) (err error) {
	url := topo.VerifyURL + directory
	payload := strings.NewReader(`{"dir":"true"}`)
	switch command {
	case "mkdir":
		if *topoFlavor == "etcd2" { // No need to create the empty prefix keys in v3
			return nil
		}
		req, _ := http.NewRequest("PUT", url, payload)
		req.Header.Add("content-type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			defer resp.Body.Close()
		}
		return err
	case "rmdir":
		if *topoFlavor == "etcd2" {
			if topo.Client == nil {
				return errors.New("etcd client is not initialized")
			}
			cli, ok := topo.Client.(*clientv3.Client)
			if !ok {
				return errors.New("etcd client is invalid")
			}
			ctx, cancel := context.WithTimeout(context.Background(), vtopo.RemoteOperationTimeout)
			defer cancel()
			_, err = cli.Delete(ctx, directory, clientv3.WithPrefix())
			if err != nil {
				return err
			}
			return nil
		}
		req, _ := http.NewRequest("DELETE", url+"?dir=true", payload)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			defer resp.Body.Close()
		}
		return err
	default:
		return nil
	}
}

// TopoProcessInstance returns a TopoProcess handle for a etcd sevice,
// configured with the given Config.
// The process must be manually started by calling setup()
func TopoProcessInstance(port int, peerPort int, hostname string, flavor string, name string) *TopoProcess {
	binary := "etcd"
	if flavor == "zk2" {
		binary = "zkctl"
	}

	topo := &TopoProcess{
		Name:   name,
		Binary: binary,
		Port:   port,
		Host:   hostname,
	}

	topo.AdvertiseClientURL = fmt.Sprintf("http://%s:%d", topo.Host, topo.Port)
	topo.ListenClientURL = fmt.Sprintf("http://%s:%d", topo.Host, topo.Port)
	topo.DataDirectory = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("%s_%d", "topo", port))
	topo.LogDirectory = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("%s_%d", "topo", port), "logs")
	topo.VerifyURL = fmt.Sprintf("http://%s:%d/health", topo.Host, topo.Port)
	topo.PeerURL = fmt.Sprintf("http://%s:%d", hostname, peerPort)
	return topo
}

// TopoGlobalRoot returns the global root for the given topo flavor.
func TopoGlobalRoot(flavor string) string {
	return "/vitess/global"
}
