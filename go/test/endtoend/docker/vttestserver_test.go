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
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"

	"github.com/stretchr/testify/require"
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
	dockerImages := []string{vttestserverMysql57image, vttestserverMysql80image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			vtest := newVttestserver(image, []string{"unsharded_ks"}, []int{1}, 1000, 33577)
			err := vtest.startDockerImage()
			require.NoError(t, err)
			defer vtest.teardown()

			// wait for the docker to be setup
			err = vtest.waitUntilDockerHealthy(10)
			require.NoError(t, err)

			ctx := context.Background()
			vttestParams := mysql.ConnParams{
				Host: "localhost",
				Port: vtest.port,
			}
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()
			utils.AssertMatches(t, conn, "show databases", `[[VARCHAR("unsharded_ks")] [VARCHAR("information_schema")] [VARCHAR("mysql")] [VARCHAR("sys")] [VARCHAR("performance_schema")]]`)
			utils.Exec(t, conn, "create table unsharded_ks.t1(id int)")
			utils.Exec(t, conn, "insert into unsharded_ks.t1(id) values (10),(20),(30)")
			utils.AssertMatches(t, conn, "select * from unsharded_ks.t1", `[[INT32(10)] [INT32(20)] [INT32(30)]]`)
		})
	}
}

func TestSharded(t *testing.T) {
	dockerImages := []string{vttestserverMysql57image, vttestserverMysql80image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			vtest := newVttestserver(image, []string{"ks"}, []int{2}, 1000, 33577)
			err := vtest.startDockerImage()
			require.NoError(t, err)
			defer vtest.teardown()

			// wait for the docker to be setup
			err = vtest.waitUntilDockerHealthy(10)
			require.NoError(t, err)

			ctx := context.Background()
			vttestParams := mysql.ConnParams{
				Host: "localhost",
				Port: vtest.port,
			}
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()
			utils.AssertMatches(t, conn, "show databases", `[[VARCHAR("ks")] [VARCHAR("information_schema")] [VARCHAR("mysql")] [VARCHAR("sys")] [VARCHAR("performance_schema")]]`)
			utils.Exec(t, conn, "create table ks.t1(id int)")
			utils.Exec(t, conn, "alter vschema on ks.t1 add vindex `binary_md5`(id) using `binary_md5`")
			utils.Exec(t, conn, "insert into ks.t1(id) values (10),(20),(30)")
			utils.AssertMatches(t, conn, "select id from ks.t1 order by id", `[[INT32(10)] [INT32(20)] [INT32(30)]]`)
		})
	}
}

func TestMysqlMaxCons(t *testing.T) {
	dockerImages := []string{vttestserverMysql57image, vttestserverMysql80image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			vtest := newVttestserver(image, []string{"ks"}, []int{2}, 100000, 33577)
			err := vtest.startDockerImage()
			require.NoError(t, err)
			defer vtest.teardown()

			// wait for the docker to be setup
			err = vtest.waitUntilDockerHealthy(10)
			require.NoError(t, err)

			ctx := context.Background()
			vttestParams := mysql.ConnParams{
				Host: "localhost",
				Port: vtest.port,
			}
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()
			utils.AssertMatches(t, conn, "select @@max_connections", `[[UINT64(100000)]]`)
		})
	}
}

func TestLargeNumberOfKeyspaces(t *testing.T) {
	dockerImages := []string{vttestserverMysql57image, vttestserverMysql80image}
	for _, image := range dockerImages {
		t.Run(image, func(t *testing.T) {
			var keyspaces []string
			var numShards []int
			for i := 0; i < 100; i++ {
				keyspaces = append(keyspaces, fmt.Sprintf("unsharded_ks%d", i))
				numShards = append(numShards, 1)
			}

			vtest := newVttestserver(image, keyspaces, numShards, 100000, 33577)
			err := vtest.startDockerImage()
			require.NoError(t, err)
			defer vtest.teardown()

			// wait for the docker to be setup
			err = vtest.waitUntilDockerHealthy(15)
			require.NoError(t, err)

			ctx := context.Background()
			vttestParams := mysql.ConnParams{
				Host: "localhost",
				Port: vtest.port,
			}
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()

			// assert that all the keyspaces are correctly setup
			for _, keyspace := range keyspaces {
				utils.Exec(t, conn, "create table "+keyspace+".t1(id int)")
				utils.Exec(t, conn, "insert into "+keyspace+".t1(id) values (10),(20),(30)")
				utils.AssertMatches(t, conn, "select * from "+keyspace+".t1", `[[INT32(10)] [INT32(20)] [INT32(30)]]`)
			}
		})
	}
}
