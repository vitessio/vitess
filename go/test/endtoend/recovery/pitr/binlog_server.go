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

package pitr

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

const (
	binlogExecutableName = "rippled"
	binlogDataDir        = "binlog_dir"
	binlogUser           = "ripple"
	binlogPassword       = "ripplepassword"
	binlogPasswordHash   = "D4CDF66E273494CEA9592162BEBB6D62D94C4168"
)

type binLogServer struct {
	hostname       string
	port           int
	username       string
	password       string
	passwordHash   string
	dataDirectory  string
	executablePath string

	proc *exec.Cmd
	exit chan error
}

type mysqlMaster struct {
	hostname string
	port     int
	username string
	password string
}

// newBinlogServer returns an instance of binlog server
func newBinlogServer(hostname string, port int) (*binLogServer, error) {
	dataDir := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("%s_%d", binlogDataDir, port))
	fmt.Println(dataDir)
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		err := os.Mkdir(dataDir, 0700)
		if err != nil {
			log.Error(err)
			return nil, err
		}
	}
	return &binLogServer{
		executablePath: path.Join(os.Getenv("EXTRA_BIN"), binlogExecutableName),
		dataDirectory:  dataDir,
		username:       binlogUser,
		password:       binlogPassword,
		passwordHash:   binlogPasswordHash,
		hostname:       hostname,
		port:           port,
	}, nil
}

// start starts the binlog server points to running mysql port
func (bs *binLogServer) start(master mysqlMaster) error {
	bs.proc = exec.Command(
		bs.executablePath,
		fmt.Sprintf("-ripple_datadir=%s", bs.dataDirectory),
		fmt.Sprintf("-ripple_server_password_hash=%s", bs.passwordHash),
		fmt.Sprintf("-ripple_master_address=%s", master.hostname),
		fmt.Sprintf("-ripple_master_port=%d", master.port),
		fmt.Sprintf("-ripple_master_user=%s", master.username),
		fmt.Sprintf("-ripple_server_ports=%d", bs.port),
	)
	if master.password != "" {
		bs.proc.Args = append(bs.proc.Args, fmt.Sprintf("-ripple_master_password=%s", master.password))
	}

	errFile, _ := os.Create(path.Join(bs.dataDirectory, "log.txt"))
	bs.proc.Stderr = errFile

	bs.proc.Env = append(bs.proc.Env, os.Environ()...)

	log.Infof("Running binlog server with command: %v", strings.Join(bs.proc.Args, " "))

	err := bs.proc.Start()
	if err != nil {
		return err
	}
	bs.exit = make(chan error)
	go func() {
		if bs.proc != nil {
			bs.exit <- bs.proc.Wait()
		}
	}()
	return nil
}

func (bs *binLogServer) stop() error {
	if bs.proc == nil || bs.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	bs.proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-bs.exit:
		bs.proc = nil
		return err

	case <-time.After(10 * time.Second):
		bs.proc.Process.Kill()
		bs.proc = nil
		return <-bs.exit
	}
}
