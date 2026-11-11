/*
Copyright 2026 The Vitess Authors.

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

package vtctldclient

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// placeholderUser and placeholderPassword are stand-ins for a vtctld static
// credential. They are not a real secret.
const (
	placeholderUser     = "placeholder-user"
	placeholderPassword = "placeholder-password"
)

// TestVtctldProxyPasswordNotOnPlaintext loads vtctld credentials and calls
// GetKeyspaces with the default dial options, which leave TLS unset. Every
// other VtctldClient method uses that same dial.
func TestVtctldProxyPasswordNotOnPlaintext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tap := &tapListener{Listener: ln}

	server := grpc.NewServer()
	vtctlservicepb.RegisterVtctldServer(server, vtctlservicepb.UnimplementedVtctldServer{})
	go server.Serve(tap)
	t.Cleanup(func() {
		server.Stop()
		_ = ln.Close()
	})

	path := writePlaceholderCreds(t)
	disco := fakediscovery.New()
	disco.AddTaggedVtctlds(nil, &vtadminpb.Vtctld{Hostname: ln.Addr().String()})

	cfg, err := Parse(&vtadminpb.Cluster{Id: "vtctldplain", Name: "vtctldplain"}, disco, []string{
		"--credentials-path-tmpl=" + path,
		"--discovery-timeout=5s",
	})
	require.NoError(t, err)
	require.NotNil(t, cfg.Credentials)
	require.NotEmpty(t, cfg.Credentials.Username)

	proxy, err := New(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = proxy.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	_, callErr := proxy.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	sawHeaders, passwordSent := passwordHeadersOnPlaintext(tap.clientBytes())
	if passwordSent {
		t.Fatal("vtctld password metadata was readable on a plaintext HTTP/2 connection")
	}
	if callErr == nil && !sawHeaders {
		t.Fatal("vtctld call succeeded but no plaintext HTTP/2 headers were captured")
	}
}

func writePlaceholderCreds(t *testing.T) string {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "creds-*.json")
	require.NoError(t, err)
	_, err = f.WriteString(`{"Username":"` + placeholderUser + `","Password":"` + placeholderPassword + `"}`)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

type tapListener struct {
	net.Listener

	mu    sync.Mutex
	conns []*tapConn
}

func (l *tapListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tc := &tapConn{Conn: c}
	l.mu.Lock()
	l.conns = append(l.conns, tc)
	l.mu.Unlock()
	return tc, nil
}

func (l *tapListener) clientBytes() [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([][]byte, len(l.conns))
	for i, c := range l.conns {
		c.mu.Lock()
		out[i] = append([]byte(nil), c.buf.Bytes()...)
		c.mu.Unlock()
	}
	return out
}

type tapConn struct {
	net.Conn

	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *tapConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.mu.Lock()
		_, _ = c.buf.Write(p[:n])
		c.mu.Unlock()
	}
	return n, err
}

func passwordHeadersOnPlaintext(conns [][]byte) (sawHeaders bool, passwordSent bool) {
	for _, raw := range conns {
		if !bytes.HasPrefix(raw, []byte(http2.ClientPreface)) {
			continue
		}
		payload := raw[len(http2.ClientPreface):]
		fr := http2.NewFramer(io.Discard, bytes.NewReader(payload))
		fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
		for {
			frame, err := fr.ReadFrame()
			if err != nil {
				break
			}
			mh, ok := frame.(*http2.MetaHeadersFrame)
			if !ok {
				continue
			}
			sawHeaders = true
			for _, field := range mh.Fields {
				if field.Name == "password" && field.Value != "" {
					passwordSent = true
				}
			}
		}
	}
	return sawHeaders, passwordSent
}
