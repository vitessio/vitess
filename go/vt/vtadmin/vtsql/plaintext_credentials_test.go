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

package vtsql

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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery/fakediscovery"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// placeholderUser and placeholderPassword are stand-ins for a vtgate static
// credential. They are not a real secret.
const (
	placeholderUser     = "placeholder-user"
	placeholderPassword = "placeholder-password"
)

// TestVTGateProxyPasswordNotOnPlaintext dials a vtgate the way VTAdmin does
// and reads the HTTP/2 headers off the plaintext connection. ShowTablets,
// Ping, PingContext, and both VExplain paths share VTGateProxy.dial.
func TestVTGateProxyPasswordNotOnPlaintext(t *testing.T) {
	listener := startPlaintextVTGate(t)

	t.Run("flags", func(t *testing.T) {
		proxy := dialVTGateProxy(t, listener.Addr().String(), "flags", []string{
			"--credentials-username=" + placeholderUser,
			"--credentials-password=" + placeholderPassword,
			"--discovery-timeout=5s",
		})
		exerciseVTGateProxy(t, proxy, listener)
	})

	t.Run("credentials file", func(t *testing.T) {
		path := writePlaceholderCreds(t)
		proxy := dialVTGateProxy(t, listener.Addr().String(), "file", []string{
			"--credentials-path-tmpl=" + path,
			"--discovery-timeout=5s",
		})
		exerciseVTGateProxy(t, proxy, listener)
	})
}

func dialVTGateProxy(t *testing.T, addr, clusterID string, args []string) *VTGateProxy {
	t.Helper()

	disco := fakediscovery.New()
	disco.AddTaggedGates(nil, &vtadminpb.VTGate{Hostname: addr})
	cfg, err := Parse(&vtadminpb.Cluster{Id: clusterID, Name: clusterID}, disco, args)
	require.NoError(t, err)
	require.NotNil(t, cfg.Credentials)
	require.NotEmpty(t, cfg.Credentials.GetUsername())

	proxy, err := New(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = proxy.Close()
		vtgateconn.DeregisterDialer("grpc_" + clusterID)
	})
	return proxy
}

func exerciseVTGateProxy(t *testing.T, proxy *VTGateProxy, lis *tapListener) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	before := len(lis.clientBytes())
	anyOK := false

	rows, err := proxy.ShowTablets(ctx)
	if rows != nil {
		rows.Close()
	}
	anyOK = anyOK || err == nil

	anyOK = anyOK || proxy.PingContext(ctx) == nil
	anyOK = anyOK || proxy.Ping() == nil

	parser := sqlparser.NewTestParser()
	for _, query := range []string{
		"vexplain plan select * from customers",
		"vexplain all select * from customers",
	} {
		stmt, parseErr := parser.Parse(query)
		require.NoError(t, parseErr)
		_, err = proxy.VExplain(ctx, query, stmt.(*sqlparser.VExplainStmt))
		anyOK = anyOK || err == nil
	}

	sawHeaders, passwordSent := passwordHeadersOnPlaintext(t, lis.clientBytes()[before:])
	if passwordSent {
		t.Fatal("vtgate password metadata was readable on a plaintext HTTP/2 connection")
	}
	if anyOK && !sawHeaders {
		t.Fatal("vtgate query succeeded but no plaintext HTTP/2 headers were captured")
	}
}

func startPlaintextVTGate(t *testing.T) *tapListener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tap := &tapListener{Listener: ln}

	server := grpc.NewServer()
	grpcvtgateservice.RegisterForTest(server, emptyGate{})
	go server.Serve(tap)

	t.Cleanup(func() {
		server.Stop()
		_ = ln.Close()
	})
	return tap
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

// tapListener records bytes the server reads, which are the bytes the client
// wrote on the TCP connection.
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

// passwordHeadersOnPlaintext decodes HTTP/2 header blocks with the same
// framer and HPACK decoder gRPC uses, and reports whether a non-empty
// password header was sent on the plaintext connection.
func passwordHeadersOnPlaintext(t *testing.T, conns [][]byte) (sawHeaders bool, passwordSent bool) {
	t.Helper()

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

// emptyGate accepts the queries VTGateProxy issues and returns no rows.
type emptyGate struct{}

func (emptyGate) Execute(_ context.Context, _ vtgateservice.MySQLConnection, session *vtgatepb.Session, _ string, _ map[string]*querypb.BindVariable, _ bool) (*vtgatepb.Session, *sqltypes.Result, error) {
	return session, &sqltypes.Result{}, nil
}

func (emptyGate) ExecuteBatch(_ context.Context, session *vtgatepb.Session, _ []string, _ []map[string]*querypb.BindVariable) (*vtgatepb.Session, []sqltypes.QueryResponse, error) {
	return session, nil, nil
}

func (emptyGate) StreamExecute(_ context.Context, _ vtgateservice.MySQLConnection, session *vtgatepb.Session, _ string, _ map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) (*vtgatepb.Session, error) {
	if callback != nil {
		if err := callback(&sqltypes.Result{}); err != nil {
			return session, err
		}
	}
	return session, nil
}

func (emptyGate) Prepare(_ context.Context, session *vtgatepb.Session, _ string) (*vtgatepb.Session, []*querypb.Field, uint16, error) {
	return session, nil, 0, nil
}

func (emptyGate) ExecuteMulti(_ context.Context, _ vtgateservice.MySQLConnection, session *vtgatepb.Session, _ string) (*vtgatepb.Session, []*sqltypes.Result, error) {
	return session, []*sqltypes.Result{{}}, nil
}

func (emptyGate) StreamExecuteMulti(_ context.Context, _ vtgateservice.MySQLConnection, session *vtgatepb.Session, _ string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) (*vtgatepb.Session, error) {
	if callback != nil {
		if err := callback(sqltypes.QueryResponse{}, false, true); err != nil {
			return session, err
		}
	}
	return session, nil
}

func (emptyGate) CloseSession(_ context.Context, _ *vtgatepb.Session) error { return nil }

func (emptyGate) VStream(context.Context, topodatapb.TabletType, *binlogdatapb.VGtid, *binlogdatapb.Filter, *vtgatepb.VStreamFlags, func([]*binlogdatapb.VEvent) error) error {
	return nil
}

func (emptyGate) BinlogDumpGTID(context.Context, *vtgatepb.BinlogDumpGTIDRequest, func(*vtgatepb.BinlogDumpResponse) error) error {
	return nil
}

func (emptyGate) HandlePanic(*error) {}

var _ vtgateservice.VTGateService = emptyGate{}
