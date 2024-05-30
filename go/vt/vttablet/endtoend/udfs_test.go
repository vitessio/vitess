/*
Copyright 2024 The Vitess Authors.

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

package endtoend

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"
)

const (
	soFileName = "udf.so"
	udfRows    = "select * from _vt.udfs"
)

// TestUDFs will validate that UDFs signal is sent through the stream health.
func TestUDFs(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	copySOFile(t, client)

	ch := make(chan any)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := client.StreamHealthWithContext(ctx, func(shr *querypb.StreamHealthResponse) error {
			if shr.RealtimeStats.UdfsChanged {
				ch <- true
			}
			return nil
		})
		require.NoError(t, err)
	}()

	// create a user defined function directly on mysql as it is not supported by vitess parser.
	err := cluster.Execute([]string{"CREATE AGGREGATE FUNCTION myudf RETURNS REAL SONAME 'udf.so';"}, "vttest")
	require.NoError(t, err)

	validateHealthStreamSignal(t, client, ch,
		`[[BINARY("myudf") INT8(1) BINARY("udf.so") ENUM("aggregate")]]`,
		`[[VARBINARY("myudf") VARBINARY("double") VARBINARY("aggregate")]]`)

	// dropping the user defined function.
	err = cluster.Execute([]string{"drop function myudf"}, "vttest")
	require.NoError(t, err)

	validateHealthStreamSignal(t, client, ch,
		`[]`,
		`[]`)
}

func validateHealthStreamSignal(t *testing.T, client *framework.QueryClient, ch chan any, expected ...string) {
	t.Helper()

	// validate the row in mysql.func.
	qr, err := client.Execute("select * from mysql.func", nil)
	require.NoError(t, err)
	require.Equal(t, expected[0], fmt.Sprintf("%v", qr.Rows))

	// wait for udf update
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for udf create signal")
	}

	// validate the row in _vt.udfs.
	qr, err = client.Execute(udfRows, nil)
	require.NoError(t, err)
	require.Equal(t, expected[1], fmt.Sprintf("%v", qr.Rows))
}

// TestUDF_RPC will validate that UDFs are received through the rpc call.
func TestUDF_RPC(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	copySOFile(t, client)

	// create a user defined function directly on mysql as it is not supported by vitess parser.
	err := cluster.Execute([]string{"CREATE AGGREGATE FUNCTION myudf RETURNS REAL SONAME 'udf.so';"}, "vttest")
	require.NoError(t, err)

	validateRPC(t, client, func(udfs []*querypb.UDFInfo) bool {
		// keep checking till the udf is added.
		return len(udfs) == 0
	})

	// dropping the user defined function.
	err = cluster.Execute([]string{"drop function myudf"}, "vttest")
	require.NoError(t, err)

	validateRPC(t, client, func(udfs []*querypb.UDFInfo) bool {
		// keep checking till the udf is removed.
		return len(udfs) != 0
	})
}

func validateRPC(t *testing.T, client *framework.QueryClient, cond func(udfs []*querypb.UDFInfo) bool) (<-chan time.Time, bool) {
	timeout := time.After(30 * time.Second)
	conditionNotMet := true
	for conditionNotMet {
		time.Sleep(1 * time.Second)
		select {
		case <-timeout:
			t.Fatal("timed out waiting for updated udf")
		default:
			schemaDef, udfs, err := client.GetSchema(querypb.SchemaTableType_UDFS, "")
			require.NoError(t, err)
			require.Empty(t, schemaDef)
			conditionNotMet = cond(udfs)
		}
	}
	return timeout, conditionNotMet
}

func copySOFile(t *testing.T, client *framework.QueryClient) {
	t.Helper()
	qr, err := client.Execute("select @@plugin_dir", nil)
	require.NoError(t, err)
	pluginDir := qr.Rows[0][0].ToString()

	source, err := os.Open(soFileName)
	require.NoError(t, err)
	defer source.Close()

	destination, err := os.Create(pluginDir + soFileName)
	if err != nil && strings.Contains(err.Error(), "permission denied") {
		t.Skip("permission denied to copy so file")
	}
	require.NoError(t, err)
	defer destination.Close()

	_, err = io.Copy(destination, source)
	require.NoError(t, err)
}
