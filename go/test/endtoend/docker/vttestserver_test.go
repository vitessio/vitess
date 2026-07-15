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
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

func TestMain(m *testing.M) {
	exitCode := func() int {
		err := makeVttestserverDockerImages()
		if err != nil {
			return 1
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestUnsharded(t *testing.T) {
	dockerImages := []string{vttestserverMysql80image, vttestserverMysql84image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			ctx := t.Context()
			vtest := newVttestserver(image, []string{"unsharded_ks"}, []int{1}, 1000, 33574)
			err := vtest.startDockerImage(ctx)
			require.NoError(t, err)
			defer vtest.teardown(ctx)

			vttestParams, err := vtest.mysqlConnParams(ctx)
			require.NoError(t, err)
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()
			vitesst.AssertMatches(t, conn, "show databases", `[[VARCHAR("unsharded_ks")] [VARCHAR("information_schema")] [VARCHAR("mysql")] [VARCHAR("sys")] [VARCHAR("performance_schema")]]`)
			vitesst.Exec(t, conn, "create table unsharded_ks.t1(id int)")
			vitesst.Exec(t, conn, "insert into unsharded_ks.t1(id) values (10),(20),(30)")
			vitesst.AssertMatches(t, conn, "select * from unsharded_ks.t1", `[[INT32(10)] [INT32(20)] [INT32(30)]]`)
		})
	}
}

func TestSharded(t *testing.T) {
	dockerImages := []string{vttestserverMysql80image, vttestserverMysql84image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			ctx := t.Context()
			vtest := newVttestserver(image, []string{"ks"}, []int{2}, 1000, 33574)
			err := vtest.startDockerImage(ctx)
			require.NoError(t, err)
			defer vtest.teardown(ctx)

			vttestParams, err := vtest.mysqlConnParams(ctx)
			require.NoError(t, err)
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()
			vitesst.AssertMatches(t, conn, "show databases", `[[VARCHAR("ks")] [VARCHAR("information_schema")] [VARCHAR("mysql")] [VARCHAR("sys")] [VARCHAR("performance_schema")]]`)
			vitesst.Exec(t, conn, "create table ks.t1(id int)")
			vitesst.Exec(t, conn, "alter vschema on ks.t1 add vindex `binary_md5`(id) using `binary_md5`")
			vitesst.Exec(t, conn, "insert into ks.t1(id) values (10),(20),(30)")
			vitesst.AssertMatches(t, conn, "select id from ks.t1 order by id", `[[INT32(10)] [INT32(20)] [INT32(30)]]`)
		})
	}
}

func TestMysqlMaxCons(t *testing.T) {
	dockerImages := []string{vttestserverMysql80image, vttestserverMysql84image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			ctx := t.Context()
			vtest := newVttestserver(image, []string{"ks"}, []int{2}, 100000, 33574)
			err := vtest.startDockerImage(ctx)
			require.NoError(t, err)
			defer vtest.teardown(ctx)

			vttestParams, err := vtest.mysqlConnParams(ctx)
			require.NoError(t, err)
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()
			vitesst.AssertMatches(t, conn, "select @@max_connections", `[[UINT64(100000)]]`)
		})
	}
}

// TestVtctldCommands tests that vtctld commands can be run with the docker image.
func TestVtctldCommands(t *testing.T) {
	dockerImages := []string{vttestserverMysql80image, vttestserverMysql84image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			ctx := t.Context()
			vtest := newVttestserver(image, []string{"long_ks_name"}, []int{2}, 100, 33574)
			err := vtest.startDockerImage(ctx)
			require.NoError(t, err)
			defer vtest.teardown(ctx)

			grpcAddr, err := vtest.grpcAddr(ctx)
			require.NoError(t, err)
			res, err := exec.Command("vtctldclient", "--server", grpcAddr, "GetKeyspaces").CombinedOutput()
			require.NoError(t, err, string(res))
			// We verify that the command succeeds, and the keyspace name is present in the output.
			require.Contains(t, string(res), "long_ks_name")
		})
	}
}

func TestLargeNumberOfKeyspaces(t *testing.T) {
	dockerImages := []string{vttestserverMysql80image, vttestserverMysql84image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			ctx := t.Context()
			var keyspaces []string
			var numShards []int
			for i := range 100 {
				keyspaces = append(keyspaces, fmt.Sprintf("unsharded_ks%d", i))
				numShards = append(numShards, 1)
			}

			vtest := newVttestserver(image, keyspaces, numShards, 100000, 33574)
			err := vtest.startDockerImage(ctx)
			require.NoError(t, err)
			defer vtest.teardown(ctx)

			vttestParams, err := vtest.mysqlConnParams(ctx)
			require.NoError(t, err)
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()

			// assert that all the keyspaces are correctly setup
			for _, keyspace := range keyspaces {
				vitesst.Exec(t, conn, "create table "+keyspace+".t1(id int)")
				vitesst.Exec(t, conn, "insert into "+keyspace+".t1(id) values (10),(20),(30)")
				vitesst.AssertMatches(t, conn, "select * from "+keyspace+".t1", `[[INT32(10)] [INT32(20)] [INT32(30)]]`)
			}
		})
	}
}
