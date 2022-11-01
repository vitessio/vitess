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

type BinLogServer struct {
	Hostname       string
	Port           int
	Username       string
	Password       string
	PasswordHash   string
	DataDirectory  string
	ExecutablePath string

	Proc *exec.Cmd
	Exit chan error
}

type MysqlSource struct {
	Hostname string
	Port     int
	Username string
	Password string
}

// NewBinlogServer returns an instance of binlog server
func NewBinlogServer(hostname string, port int) (*BinLogServer, error) {
	dataDir := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("%s_%d", binlogDataDir, port))
	fmt.Println(dataDir)
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		err := os.Mkdir(dataDir, 0700)
		if err != nil {
			log.Error(err)
			return nil, err
		}
	}
	return &BinLogServer{
		ExecutablePath: path.Join(os.Getenv("EXTRA_BIN"), binlogExecutableName),
		DataDirectory:  dataDir,
		Username:       binlogUser,
		Password:       binlogPassword,
		PasswordHash:   binlogPasswordHash,
		Hostname:       hostname,
		Port:           port,
	}, nil
}

// start starts the binlog server points to running mysql port
func (bs *BinLogServer) Start(source MysqlSource) error {
	bs.Proc = exec.Command(
		bs.ExecutablePath,
		fmt.Sprintf("-ripple_datadir=%s", bs.DataDirectory),
		fmt.Sprintf("-ripple_server_password_hash=%s", bs.PasswordHash),
		fmt.Sprintf("-ripple_master_address=%s", source.Hostname),
		fmt.Sprintf("-ripple_master_port=%d", source.Port),
		fmt.Sprintf("-ripple_master_user=%s", source.Username),
		fmt.Sprintf("-ripple_server_ports=%d", bs.Port),
	)
	if source.Password != "" {
		bs.Proc.Args = append(bs.Proc.Args, fmt.Sprintf("-ripple_master_password=%s", source.Password))
	}

	errFile, _ := os.Create(path.Join(bs.DataDirectory, "log.txt"))
	bs.Proc.Stderr = errFile

	bs.Proc.Env = append(bs.Proc.Env, os.Environ()...)

	log.Infof("Running binlog server with command: %v", strings.Join(bs.Proc.Args, " "))

	err := bs.Proc.Start()
	if err != nil {
		return err
	}
	bs.Exit = make(chan error)
	go func() {
		if bs.Proc != nil {
			bs.Exit <- bs.Proc.Wait()
		}
	}()
	return nil
}

func (bs *BinLogServer) Stop() error {
	if bs.Proc == nil || bs.Exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	bs.Proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-bs.Exit:
		bs.Proc = nil
		return err

	case <-time.After(10 * time.Second):
		bs.Proc.Process.Kill()
		bs.Proc = nil
		return <-bs.Exit
	}
}
