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

package vault

import (
	"bytes"
	"fmt"
	"io"
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

const (
	vaultExecutableName = "vault"
	vaultDownloadSource = "https://vitess-operator.storage.googleapis.com/install/vault"
	vaultDownloadSize   = 132738840
	vaultDirName        = "vault"
	vaultConfigFileName = "vault.hcl"
	vaultCertFileName   = "vault-cert.pem"
	vaultCAFileName     = "ca.pem"
	vaultKeyFileName    = "vault-key.pem"
	vaultSetupScript    = "vault-setup.sh"
)

// VaultServer : Basic parameters for the running the Vault server
type VaultServer struct {
	address  string
	port1    int
	port2    int
	execPath string
	logDir   string

	proc *exec.Cmd
	exit chan error
}

// Start the Vault server in dev mode
func (vs *VaultServer) start() error {
	// Download and unpack vault binary
	vs.execPath = path.Join(os.Getenv("EXTRA_BIN"), vaultExecutableName)
	fileStat, err := os.Stat(vs.execPath)
	if err != nil || fileStat.Size() != vaultDownloadSize {
		log.Warningf("Downloading Vault binary to: %v", vs.execPath)
		err := downloadExecFile(vs.execPath, vaultDownloadSource)
		if err != nil {
			log.Error(err)
			return err
		}
	} else {
		log.Warningf("Vault binary already present at %v , not re-downloading", vs.execPath)
	}

	// Create Vault log directory
	vs.logDir = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("%s_%d", vaultDirName, vs.port1))
	if _, err := os.Stat(vs.logDir); os.IsNotExist(err) {
		err := os.Mkdir(vs.logDir, 0700)
		if err != nil {
			log.Error(err)
			return err
		}
	}

	hclFile := path.Join(os.Getenv("PWD"), vaultConfigFileName)
	hcl, _ := ioutil.ReadFile(hclFile)
	// Replace variable parts in Vault config file
	hcl = bytes.Replace(hcl, []byte("$server"), []byte(vs.address), 1)
	hcl = bytes.Replace(hcl, []byte("$port"), []byte(fmt.Sprintf("%d", vs.port1)), 1)
	hcl = bytes.Replace(hcl, []byte("$cert"), []byte(path.Join(os.Getenv("PWD"), vaultCertFileName)), 1)
	hcl = bytes.Replace(hcl, []byte("$key"), []byte(path.Join(os.Getenv("PWD"), vaultKeyFileName)), 1)
	newHclFile := path.Join(vs.logDir, vaultConfigFileName)
	err = ioutil.WriteFile(newHclFile, hcl, 0700)
	if err != nil {
		log.Error(err)
		return err
	}

	vs.proc = exec.Command(
		vs.execPath,
		"server",
		fmt.Sprintf("-config=%s", newHclFile),
	)

	logFile, err := os.Create(path.Join(vs.logDir, "log.txt"))
	if err != nil {
		log.Error(err)
		return err
	}
	vs.proc.Stderr = logFile
	vs.proc.Stdout = logFile

	vs.proc.Env = append(vs.proc.Env, os.Environ()...)

	log.Infof("Running Vault server with command: %v", strings.Join(vs.proc.Args, " "))

	err = vs.proc.Start()
	if err != nil {
		return err
	}
	vs.exit = make(chan error)
	go func() {
		if vs.proc != nil {
			vs.exit <- vs.proc.Wait()
		}
	}()
	return nil
}

func (vs *VaultServer) stop() error {
	if vs.proc == nil || vs.exit == nil {
		return nil
	}
	// Attempt graceful shutdown with SIGTERM first
	vs.proc.Process.Signal(syscall.SIGTERM)

	select {
	case err := <-vs.exit:
		vs.proc = nil
		return err

	case <-time.After(10 * time.Second):
		vs.proc.Process.Kill()
		vs.proc = nil
		return <-vs.exit
	}
}

// Download file from url to path; making it executable
func downloadExecFile(path string, url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	err = ioutil.WriteFile(path, []byte(""), 0700)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
