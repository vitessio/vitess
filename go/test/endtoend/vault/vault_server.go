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
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"vitess.io/vitess/go/vt/log"
)

const (
	vaultExecutableName = "vault"
	vaultVersion        = "1.6.1"
	vaultDirName        = "vault"
	vaultConfigFileName = "vault.hcl"
	vaultCertFileName   = "vault-cert.pem"
	vaultCAFileName     = "ca.pem"
	vaultKeyFileName    = "vault-key.pem"
	vaultSetupScript    = "vault-setup.sh"
)

// vaultReleaseChecksums holds the sha256 of each Vault release zip we can run,
// keyed by GOOS/GOARCH, taken from
// https://releases.hashicorp.com/vault/1.6.1/vault_1.6.1_SHA256SUMS
var vaultReleaseChecksums = map[string]string{
	"linux/amd64": "75cd2b8c5527577c0da1105e11fba3c31f4112514a910c4f7ec527c9a8bf42d1",
	"linux/arm64": "09e9fc0a69350d49a5db90c51b19a2a63b1da060eeeed700109fb43e544ba947",
}

// vaultReleaseArtifact is the Vault release zip for one platform.
type vaultReleaseArtifact struct {
	url    string
	sha256 string
}

// vaultRelease returns the Vault release zip for the given platform.
func vaultRelease(goos, goarch string) (vaultReleaseArtifact, error) {
	platform := fmt.Sprintf("%s/%s", goos, goarch)
	sum, ok := vaultReleaseChecksums[platform]
	if !ok {
		return vaultReleaseArtifact{}, fmt.Errorf("no Vault %s release known for %s", vaultVersion, platform)
	}
	return vaultReleaseArtifact{
		url:    fmt.Sprintf("https://releases.hashicorp.com/vault/%s/vault_%s_%s_%s.zip", vaultVersion, vaultVersion, goos, goarch),
		sha256: sum,
	}, nil
}

// Server : Basic parameters for the running the Vault server
type Server struct {
	address  string
	port1    int
	port2    int
	execPath string
	logDir   string

	proc *exec.Cmd
	exit chan error
}

// Start the Vault server in dev mode
func (vs *Server) start() error {
	// Download and unpack vault binary
	vs.execPath = path.Join(os.Getenv("EXTRA_BIN"), vaultExecutableName)
	_, err := os.Stat(vs.execPath)
	if err != nil {
		release, err := vaultRelease(runtime.GOOS, runtime.GOARCH)
		if err != nil {
			log.Error(fmt.Sprint(err))
			return err
		}
		log.Warn(fmt.Sprintf("Downloading Vault binary from %s to: %v", release.url, vs.execPath))
		err = downloadVault(vs.execPath, release)
		if err != nil {
			log.Error(fmt.Sprint(err))
			return err
		}
	} else {
		log.Warn(fmt.Sprintf("Vault binary already present at %v , not re-downloading", vs.execPath))
	}

	// Create Vault log directory
	vs.logDir = path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("%s_%d", vaultDirName, vs.port1))
	if _, err := os.Stat(vs.logDir); os.IsNotExist(err) {
		err := os.Mkdir(vs.logDir, 0o700)
		if err != nil {
			log.Error(fmt.Sprint(err))
			return err
		}
	}

	hclFile := path.Join(os.Getenv("PWD"), vaultConfigFileName)
	hcl, _ := os.ReadFile(hclFile)
	// Replace variable parts in Vault config file
	hcl = bytes.Replace(hcl, []byte("$server"), []byte(vs.address), 1)
	hcl = bytes.Replace(hcl, []byte("$port"), []byte(strconv.Itoa(vs.port1)), 1)
	hcl = bytes.Replace(hcl, []byte("$cert"), []byte(path.Join(os.Getenv("PWD"), vaultCertFileName)), 1)
	hcl = bytes.Replace(hcl, []byte("$key"), []byte(path.Join(os.Getenv("PWD"), vaultKeyFileName)), 1)
	newHclFile := path.Join(vs.logDir, vaultConfigFileName)
	err = os.WriteFile(newHclFile, hcl, 0o700)
	if err != nil {
		log.Error(fmt.Sprint(err))
		return err
	}

	vs.proc = exec.Command(
		vs.execPath,
		"server",
		"-config="+newHclFile,
	)

	logFile, err := os.Create(path.Join(vs.logDir, "log.txt"))
	if err != nil {
		log.Error(fmt.Sprint(err))
		return err
	}
	vs.proc.Stderr = logFile
	vs.proc.Stdout = logFile

	vs.proc.Env = append(vs.proc.Env, os.Environ()...)

	log.Info(fmt.Sprintf("Running Vault server with command: %v", strings.Join(vs.proc.Args, " ")))

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

func (vs *Server) stop() error {
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

// downloadVault downloads the Vault release zip, verifies its checksum and
// extracts the executable to execPath. The executable only appears at execPath
// once it is complete, so a partial download is never mistaken for a binary.
func downloadVault(execPath string, release vaultReleaseArtifact) error {
	resp, err := http.Get(release.url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("downloading %s: unexpected status %s", release.url, resp.Status)
	}

	dir := path.Dir(execPath)
	zipFile, err := os.CreateTemp(dir, vaultExecutableName+"-*.zip")
	if err != nil {
		return err
	}
	defer os.Remove(zipFile.Name())
	hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(zipFile, hash), resp.Body)
	zipFile.Close()
	if err != nil {
		return err
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != release.sha256 {
		return fmt.Errorf("checksum mismatch for %s: got %s, want %s", release.url, got, release.sha256)
	}

	zipReader, err := zip.OpenReader(zipFile.Name())
	if err != nil {
		return err
	}
	defer zipReader.Close()
	for _, file := range zipReader.File {
		if file.Name != vaultExecutableName {
			continue
		}
		in, err := file.Open()
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.CreateTemp(dir, vaultExecutableName+"-*")
		if err != nil {
			return err
		}
		_, err = io.Copy(out, in)
		out.Close()
		if err != nil {
			os.Remove(out.Name())
			return err
		}
		if err := os.Chmod(out.Name(), 0o700); err != nil {
			os.Remove(out.Name())
			return err
		}
		return os.Rename(out.Name(), execPath)
	}
	return fmt.Errorf("%s does not contain a %s executable", release.url, vaultExecutableName)
}
