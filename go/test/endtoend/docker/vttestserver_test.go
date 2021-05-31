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
	"os"
	"os/exec"
	"path"
	"testing"

	"github.com/golang/glog"
)

const (
	vttestserverMysql57image = "vttestserver-e2etest/mysql57"
	vttestserverMysql80image = "vttestserver-e2etest/mysql80"
)

func TestMain(m *testing.M) {
	exitCode := func() int {
		err := makeVttestserverDockerImages()
		if err != nil {
			glog.Error(err.Error())
			return 1
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

//makeVttestserverDockerImages creates the vttestserver docker images for both MySQL57 and MySQL80
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
