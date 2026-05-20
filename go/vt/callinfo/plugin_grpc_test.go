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

package callinfo

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGRPCCallInfo(t *testing.T) {
	grpcCi := callInfoContext{
		Context:    t.Context(),
		method:     "tcp",
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080},
	}

	require.Equal(t, t.Context(), GRPCCallInfo(t.Context()))
	require.Equal(t, "127.0.0.1:8080", grpcCi.RemoteAddr())
	require.Equal(t, "gRPC", grpcCi.Username())
	require.Equal(t, "127.0.0.1:8080:tcp(gRPC)", grpcCi.Text())
	require.Equal(t, "<b>Method:</b> tcp <b>Remote Addr:</b> 127.0.0.1:8080", grpcCi.HTML().String())
}

func TestGRPCCallInfoNilAddr(t *testing.T) {
	grpcCi := callInfoContext{
		Context: t.Context(),
		method:  "test",
	}
	require.Equal(t, "", grpcCi.RemoteAddr())
}

func TestGRPCCallInfoFromContext(t *testing.T) {
	ctx := &callInfoContext{
		Context:    t.Context(),
		method:     "/queryservice.Query/Execute",
		remoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 1234},
	}
	ci, ok := FromContext(ctx)
	require.True(t, ok)
	require.Equal(t, "10.0.0.1:1234", ci.RemoteAddr())
}
