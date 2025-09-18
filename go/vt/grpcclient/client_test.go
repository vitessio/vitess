/*
Copyright 2019 The Vitess Authors.

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

package grpcclient

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/grpc"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

func TestDialErrors(t *testing.T) {
	addresses := []string{
		"badhost",
		"badhost:123456",
		"[::]:12346",
	}
	wantErr := "Unavailable"
	for _, address := range addresses {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		gconn, err := DialContext(ctx, address, true, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		vtg := vtgateservicepb.NewVitessClient(gconn)
		_, err = vtg.Execute(ctx, &vtgatepb.ExecuteRequest{})
		cancel()
		gconn.Close()
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("DialContext(%s, FailFast=true): %v, must contain %s", address, err, wantErr)
		}
	}

	wantErr = "DeadlineExceeded"
	for _, address := range addresses {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		gconn, err := DialContext(ctx, address, false, grpc.WithTransportCredentials(insecure.NewCredentials()))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		vtg := vtgateservicepb.NewVitessClient(gconn)
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err = vtg.Execute(ctx, &vtgatepb.ExecuteRequest{})
		cancel()
		gconn.Close()
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Errorf("DialContext(%s, FailFast=false): %v, must contain %s", address, err, wantErr)
		}
	}
}

func TestRegisterGRPCClientFlags(t *testing.T) {
	oldArgs := os.Args
	defer func() {
		os.Args = oldArgs
	}()

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs)

	// Test current values
	require.Equal(t, 10*time.Second, keepaliveTime)
	require.Equal(t, 10*time.Second, keepaliveTimeout)
	require.Equal(t, 0, initialWindowSize)
	require.Equal(t, 0, initialConnWindowSize)
	require.Equal(t, "", compression)
	require.Equal(t, "", credsFile)

	// Use SetFlagVariantsForTests to randomly pick dashed or underscored keys.
	flagMap := map[string]string{
		"--grpc-keepalive-time":           "5s",
		"--grpc-keepalive-timeout":        "5s",
		"--grpc-initial-window-size":      "10",
		"--grpc-initial-conn-window-size": "10",
		"--grpc-compression":              "not-snappy",
		"--grpc-auth-static-client-creds": "tempfile",
	}

	// Test setting flags from command-line arguments
	args := []string{"test"}
	for k, v := range flagMap {
		args = append(args, k+"="+v)
	}
	os.Args = args
	err := fs.Parse(os.Args[1:])
	require.NoError(t, err)

	require.Equal(t, 5*time.Second, keepaliveTime)
	require.Equal(t, 5*time.Second, keepaliveTimeout)
	require.Equal(t, 10, initialWindowSize)
	require.Equal(t, 10, initialConnWindowSize)
	require.Equal(t, "not-snappy", compression)
	require.Equal(t, "tempfile", credsFile)
}
