/*
Copyright 2021 The Vitess Authors.

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

package docker

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"
)

const (
	vttestserverMysql57image = "vttestserver-e2etest/mysql57"
	vttestserverMysql80image = "vttestserver-e2etest/mysql80"
)

type vttestserver struct {
	dockerImage          string
	keyspaces            []string
	numShards            []int
	mysqlMaxConnecetions int
	port                 int
}

func newVttestserver(dockerImage string, keyspaces []string, numShards []int, mysqlMaxConnections, port int) *vttestserver {
	return &vttestserver{
		dockerImage:          dockerImage,
		keyspaces:            keyspaces,
		numShards:            numShards,
		mysqlMaxConnecetions: mysqlMaxConnections,
		port:                 port,
	}
}

func (v *vttestserver) teardown() {
	cmd := exec.Command("docker", "rm", "--force", "vttestserver-end2end-test")
	err := cmd.Run()
	if err != nil {
		log.Errorf("docker teardown failed :- %s", err.Error())
	}
}

// startDockerImage starts the docker image for the vttestserver
func (v *vttestserver) startDockerImage() error {
	cmd := exec.Command("docker", "run")
	cmd.Args = append(cmd.Args, "--name=vttestserver-end2end-test")
	cmd.Args = append(cmd.Args, "-p", fmt.Sprintf("%d:33577", v.port))
	cmd.Args = append(cmd.Args, "-e", "PORT=33574")
	cmd.Args = append(cmd.Args, "-e", fmt.Sprintf("KEYSPACES=%s", strings.Join(v.keyspaces, ",")))
	cmd.Args = append(cmd.Args, "-e", fmt.Sprintf("NUM_SHARDS=%s", strings.Join(convertToStringSlice(v.numShards), ",")))
	cmd.Args = append(cmd.Args, "-e", "MYSQL_BIND_HOST=0.0.0.0")
	cmd.Args = append(cmd.Args, "-e", fmt.Sprintf("MYSQL_MAX_CONNECTIONS=%d", v.mysqlMaxConnecetions))
	cmd.Args = append(cmd.Args, "--health-cmd", "mysqladmin ping -h127.0.0.1 -P33577")
	cmd.Args = append(cmd.Args, "--health-interval=5s")
	cmd.Args = append(cmd.Args, "--health-timeout=2s")
	cmd.Args = append(cmd.Args, "--health-retries=5")
	cmd.Args = append(cmd.Args, v.dockerImage)

	err := cmd.Start()
	if err != nil {
		return err
	}
	return nil
}

// dockerStatus is a struct used to unmarshal json output from `docker inspect`
type dockerStatus struct {
	State struct {
		Health struct {
			Status string
		}
	}
}

// waitUntilDockerHealthy waits until the docker image is healthy. It takes in as argument the amount of seconds to wait before timeout
func (v *vttestserver) waitUntilDockerHealthy(timeoutDelay int) error {
	timeOut := time.After(time.Duration(timeoutDelay) * time.Second)

	for {
		select {
		case <-timeOut:
			// return error due to timeout
			return fmt.Errorf("timed out waiting for docker image to start")
		case <-time.After(time.Second):
			cmd := exec.Command("docker", "inspect", "vttestserver-end2end-test")
			out, err := cmd.Output()
			if err != nil {
				return err
			}
			var x []dockerStatus
			err = json.Unmarshal(out, &x)
			if err != nil {
				return err
			}
			if len(x) > 0 {
				status := x[0].State.Health.Status
				if status == "healthy" {
					return nil
				}
			}
		}
	}
}

// convertToStringSlice converts an integer slice to string slice
func convertToStringSlice(intSlice []int) []string {
	var stringSlice []string
	for _, val := range intSlice {
		str := strconv.Itoa(val)
		stringSlice = append(stringSlice, str)
	}
	return stringSlice
}

// makeVttestserverDockerImages creates the vttestserver docker images for both MySQL57 and MySQL80
func makeVttestserverDockerImages() error {
	mainVitessPath := path.Join(os.Getenv("PWD"), "../../../..")
	dockerFilePath := path.Join(mainVitessPath, "docker/vttestserver/Dockerfile.mysql57")
	cmd57 := exec.Command("docker", "build", "-f", dockerFilePath, "-t", vttestserverMysql57image, ".")
	cmd57.Dir = mainVitessPath
	err := cmd57.Start()
	if err != nil {
		return err
	}

	dockerFilePath = path.Join(mainVitessPath, "docker/vttestserver/Dockerfile.mysql80")
	cmd80 := exec.Command("docker", "build", "-f", dockerFilePath, "-t", vttestserverMysql80image, ".")
	cmd80.Dir = mainVitessPath
	err = cmd80.Start()
	if err != nil {
		return err
	}

	err = cmd57.Wait()
	if err != nil {
		return err
	}

	err = cmd80.Wait()
	if err != nil {
		return err
	}

	return nil
}
