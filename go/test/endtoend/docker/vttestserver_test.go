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
	"time"

	"github.com/google/go-cmp/cmp"

	"vitess.io/vitess/go/sqltypes"

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
			time.Sleep(10 * time.Second)

			ctx := context.Background()
			vttestParams := mysql.ConnParams{
				Host: "localhost",
				Port: vtest.port,
			}
			conn, err := mysql.Connect(ctx, &vttestParams)
			require.NoError(t, err)
			defer conn.Close()
			assertMatches(t, conn, "show databases", `[[VARCHAR("unsharded_ks")] [VARCHAR("information_schema")] [VARCHAR("mysql")] [VARCHAR("sys")] [VARCHAR("performance_schema")]]`)
			_, err = execute(t, conn, "create table unsharded_ks.t1(id int)")
			require.NoError(t, err)
			_, err = execute(t, conn, "insert into unsharded_ks.t1(id) values (10),(20),(30)")
			require.NoError(t, err)
			assertMatches(t, conn, "select * from unsharded_ks.t1", `[[INT32(10)] [INT32(20)] [INT32(30)]]`)
		})
	}
}

func execute(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}

func checkedExec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err)
	return qr
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := checkedExec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}
