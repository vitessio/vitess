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
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	vttestserverMysql80image = "vttestserver-e2etest/mysql80"
	vttestserverMysql84image = "vttestserver-e2etest/mysql84"
)

const (
	// startupTimeout bounds how long a vttestserver container may take to
	// accept MySQL connections. It is generous to absorb slow CI runners and
	// clusters with many keyspaces.
	startupTimeout = 3 * time.Minute

	// pollInterval is how often the readiness probe runs.
	pollInterval = 100 * time.Millisecond

	// terminateTimeout bounds container teardown.
	terminateTimeout = 60 * time.Second
)

type vttestserver struct {
	dockerImage          string
	keyspaces            []string
	numShards            []int
	mysqlMaxConnecetions int
	basePort             int

	container testcontainers.Container
}

func newVttestserver(dockerImage string, keyspaces []string, numShards []int, mysqlMaxConnections, port int) *vttestserver {
	return &vttestserver{
		dockerImage:          dockerImage,
		keyspaces:            keyspaces,
		numShards:            numShards,
		mysqlMaxConnecetions: mysqlMaxConnections,
		basePort:             port,
	}
}

// teardown terminates the vttestserver container.
func (v *vttestserver) teardown(ctx context.Context) {
	if v.container == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminateTimeout)
	defer cancel()
	err := testcontainers.TerminateContainer(v.container, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0))
	if err != nil {
		log.Error("docker teardown failed :- " + err.Error())
	}
}

// startDockerImage starts the vttestserver container and blocks until its
// MySQL port accepts connections. The vtcombo ports inside the container are
// derived from PORT: http on PORT, grpc on PORT+1 and the MySQL server on
// PORT+3.
func (v *vttestserver) startDockerImage(ctx context.Context) error {
	mysqlPort := fmt.Sprintf("%d/tcp", v.basePort+3)
	ctr, err := testcontainers.Run(
		ctx, v.dockerImage,
		testcontainers.WithExposedPorts(
			fmt.Sprintf("%d/tcp", v.basePort),
			fmt.Sprintf("%d/tcp", v.basePort+1),
			mysqlPort,
		),
		testcontainers.WithEnv(map[string]string{
			"PORT":                  strconv.Itoa(v.basePort),
			"KEYSPACES":             strings.Join(v.keyspaces, ","),
			"NUM_SHARDS":            strings.Join(convertToStringSlice(v.numShards), ","),
			"MYSQL_BIND_HOST":       "0.0.0.0",
			"VTCOMBO_BIND_HOST":     "0.0.0.0",
			"MYSQL_MAX_CONNECTIONS": strconv.Itoa(v.mysqlMaxConnecetions),
		}),
		testcontainers.WithWaitStrategyAndDeadline(
			startupTimeout,
			wait.ForExec([]string{"mysqladmin", "ping", "-h127.0.0.1", fmt.Sprintf("-P%d", v.basePort+3)}).
				WithStartupTimeout(startupTimeout).
				WithPollInterval(pollInterval),
		),
	)
	if err != nil {
		return vterrors.Wrapf(err, "starting vttestserver container from image %s", v.dockerImage)
	}
	v.container = ctr
	return nil
}

// mysqlConnParams returns connection parameters for the host-reachable MySQL
// port (PORT+3) of the running container.
func (v *vttestserver) mysqlConnParams(ctx context.Context) (mysql.ConnParams, error) {
	host, port, err := v.mappedHostPort(ctx, v.basePort+3)
	if err != nil {
		return mysql.ConnParams{}, err
	}
	return mysql.ConnParams{
		Host: host,
		Port: port,
	}, nil
}

// grpcAddr returns the host-reachable "host:port" of the container's vtcombo
// gRPC port (PORT+1).
func (v *vttestserver) grpcAddr(ctx context.Context) (string, error) {
	host, port, err := v.mappedHostPort(ctx, v.basePort+1)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, port), nil
}

// mappedHostPort resolves an in-container port to its host-reachable host and
// mapped port.
func (v *vttestserver) mappedHostPort(ctx context.Context, containerPort int) (string, int, error) {
	if v.container == nil {
		return "", 0, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vttestserver container is not running")
	}
	host, err := v.container.Host(ctx)
	if err != nil {
		return "", 0, vterrors.Wrapf(err, "resolving vttestserver host")
	}
	mapped, err := v.container.MappedPort(ctx, fmt.Sprintf("%d/tcp", containerPort))
	if err != nil {
		return "", 0, vterrors.Wrapf(err, "resolving mapped port for %d", containerPort)
	}
	return host, int(mapped.Num()), nil
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

// makeVttestserverDockerImages creates the vttestserver docker images for both MySQL80 and MySQL84
func makeVttestserverDockerImages() error {
	mainVitessPath := path.Join(os.Getenv("PWD"), "../../../..")
	dockerFilePath := path.Join(mainVitessPath, "docker/vttestserver/Dockerfile.mysql80")
	cmd80 := exec.Command("docker", "build", "-f", dockerFilePath, "-t", vttestserverMysql80image, ".")
	cmd80.Dir = mainVitessPath
	err := cmd80.Start()
	if err != nil {
		return err
	}

	dockerFilePath = path.Join(mainVitessPath, "docker/vttestserver/Dockerfile.mysql84")
	cmd84 := exec.Command("docker", "build", "-f", dockerFilePath, "-t", vttestserverMysql84image, ".")
	cmd84.Dir = mainVitessPath
	err = cmd84.Start()
	if err != nil {
		return err
	}

	err = cmd80.Wait()
	if err != nil {
		return err
	}

	err = cmd84.Wait()
	if err != nil {
		return err
	}

	return nil
}
