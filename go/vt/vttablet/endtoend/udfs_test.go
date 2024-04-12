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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/vttablet/endtoend/framework"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	soFileName = "udf.so"
	udfRows    = "select function_name from _vt.udfs"
)

// Test will validate that UDFs signal is sent through the stream health.
func TestUDFs(t *testing.T) {
	client := framework.NewClient()

	client.UpdateContext(callerid.NewContext(
		context.Background(),
		&vtrpcpb.CallerID{},
		&querypb.VTGateCallerID{Username: "dev"}))

	qr, err := client.Execute("select @@plugin_dir", nil)
	require.NoError(t, err)
	pluginDir := qr.Rows[0][0].ToString()

	source, err := os.Open(soFileName)
	require.NoError(t, err)
	defer source.Close()

	destination, err := os.Create(pluginDir + soFileName)
	require.NoError(t, err)
	defer destination.Close()

	_, err = io.Copy(destination, source)
	require.NoError(t, err)

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
	err = cluster.Execute([]string{"CREATE AGGREGATE FUNCTION myudf RETURNS REAL SONAME 'udf.so';"}, "vttest")
	require.NoError(t, err)
	// validate the row in mysql.func.
	qr, err = client.Execute("select * from mysql.func", nil)
	require.NoError(t, err)
	require.Equal(t, `[[BINARY("myudf") INT8(1) BINARY("udf.so") ENUM("aggregate")]]`, fmt.Sprintf("%v", qr.Rows))

	// wait for udf update
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for udf create signal")
	}

	// validate the row in _vt.udfs.
	qr, err = client.Execute(udfRows, nil)
	require.NoError(t, err)
	require.Equal(t, `[[VARCHAR("myudf") VARCHAR("double") VARCHAR("aggregate")]]`, fmt.Sprintf("%v", qr.Rows))

	// dropping the user defined function.
	_, err = client.Execute("drop function myudf", nil)
	require.NoError(t, err)

	// wait for views update
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for udf drop signal")
	}

	// validate the row in _vt.udfs.
	qr, err = client.Execute(udfRows, nil)
	require.NoError(t, err)
	require.Equal(t, "[]", fmt.Sprintf("%v", qr.Rows))
}
