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

package grpcclient

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
	"google.golang.org/grpc/credentials/insecure"

	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtgateservicepb "vitess.io/vitess/go/vt/proto/vtgateservice"
)

// placeholderUser and placeholderPassword are stand-ins for the static auth
// client credential file. They are not a real secret.
const (
	placeholderUser     = "placeholder-user"
	placeholderPassword = "placeholder-password"
)

// TestStaticAuthPasswordNotOnPlaintext is the dial path AppendStaticAuth adds
// to every grpcclient.DialContext when a credentials file is set. The RPC is
// vtgate Execute; any other method on that connection would carry the same
// per-RPC metadata.
func TestStaticAuthPasswordNotOnPlaintext(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "creds-*.json")
	require.NoError(t, err)
	_, err = f.WriteString(`{"Username":"` + placeholderUser + `","Password":"` + placeholderPassword + `"}`)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	old := credsFile
	credsFile = f.Name()
	ResetStaticAuth()
	t.Cleanup(func() {
		ResetStaticAuth()
		credsFile = old
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tap := &tapListener{Listener: ln}
	server := grpc.NewServer()
	go server.Serve(tap)
	t.Cleanup(func() {
		server.Stop()
		_ = ln.Close()
	})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	conn, dialErr := DialContext(ctx, ln.Addr().String(), FailFast(true), grpc.WithTransportCredentials(insecure.NewCredentials()))
	var callErr error
	if dialErr == nil {
		t.Cleanup(func() { _ = conn.Close() })
		_, callErr = vtgateservicepb.NewVitessClient(conn).Execute(ctx, &vtgatepb.ExecuteRequest{})
	}

	sawHeaders, passwordSent := passwordHeadersOnPlaintext(tap.clientBytes())
	if passwordSent {
		t.Fatal("static auth password metadata was readable on a plaintext HTTP/2 connection")
	}
	if dialErr == nil && callErr == nil && !sawHeaders {
		t.Fatal("rpc succeeded but no plaintext HTTP/2 headers were captured")
	}
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
