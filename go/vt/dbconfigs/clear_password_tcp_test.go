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

package dbconfigs

import (
	"bytes"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// placeholderDBPassword is an obvious stand-in for a tablet DB password.
const placeholderDBPassword = "placeholder-db-password"

// TestTabletTCPDefaultsDoNotSendClearPassword parses the vttablet DB flags the
// way docker/mini/vttablet-mini-up.sh sets them: db-host and db-port, no
// db-ssl-mode. Each user whose connection then dials that TCP address must
// not write its password when the peer asks for mysql_clear_password.
//
// ExternalRepl is not covered. InitWithSocket does not copy the global host
// onto that user, so this flag set never dials it over TCP.
func TestTabletTCPDefaultsDoNotSendClearPassword(t *testing.T) {
	serverAddr, err := startClearPasswordListener(t)
	require.NoError(t, err)
	proxyAddr, recorded := startRecordingProxy(t, serverAddr)
	host, portStr, err := net.SplitHostPort(proxyAddr)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	saved := GlobalDBConfigs
	savedCreds := dbCredentialsFile
	t.Cleanup(func() {
		GlobalDBConfigs = saved
		dbCredentialsFile = savedCreds
	})
	GlobalDBConfigs = DBConfigs{}
	dbCredentialsFile = ""

	fs := pflag.NewFlagSet("vttablet", pflag.ContinueOnError)
	registerBaseFlags(fs)
	for _, userKey := range []string{App, AppDebug, AllPrivs, Dba, Filtered, Repl, Clone} {
		uc, cp := GlobalDBConfigs.getParams(userKey)
		registerPerUserFlags(fs, userKey, uc, cp)
	}
	args := []string{
		"--db-host", host,
		"--db-port", strconv.Itoa(port),
		"--db-connect-timeout-ms", "5000",
	}
	for _, userKey := range []string{App, AppDebug, AllPrivs, Dba, Filtered, Repl, Clone} {
		args = append(args, "--db-"+userKey+"-password", placeholderDBPassword)
	}
	require.NoError(t, fs.Parse(args))
	GlobalDBConfigs.InitWithSocket("", collations.MySQL8())

	connectors := []struct {
		name string
		get  func(*DBConfigs) Connector
	}{
		{App, (*DBConfigs).AppWithDB},
		{AppDebug, (*DBConfigs).AppDebugWithDB},
		{AllPrivs, (*DBConfigs).AllPrivsConnector},
		{Dba, (*DBConfigs).DbaConnector},
		{Filtered, (*DBConfigs).FilteredWithDB},
		{Repl, (*DBConfigs).ReplConnector},
		{Clone, (*DBConfigs).CloneConnector},
	}
	for _, tc := range connectors {
		t.Run(tc.name, func(t *testing.T) {
			before := len(recorded())
			connector := tc.get(&GlobalDBConfigs)
			conn, err := connector.Connect(t.Context())
			if conn != nil {
				conn.Close()
			}
			delta := recorded()[before:]
			require.NotEmpty(t, delta, "client never wrote a handshake response")
			require.False(t, bytes.Contains(delta, []byte(placeholderDBPassword)), "configured password was written to the peer")
			require.Error(t, err)
		})
	}
}

func startClearPasswordListener(t *testing.T) (string, error) {
	t.Helper()
	authServer := mysql.NewAuthServerStaticWithAuthMethodDescription("", "", 0, mysql.MysqlClearPassword)
	handler := clearPasswordHandler{env: vtenv.NewTestEnv()}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:0", authServer, handler, 0, 0, false, false, 0, 0, false)
	if err != nil {
		return "", err
	}
	// The peer asks for the clear password on this TCP connection.
	listener.AllowClearTextWithoutTLS.Store(true)
	go listener.Accept()
	t.Cleanup(func() { listener.Close() })
	return listener.Addr().String(), nil
}

type recordedBytes struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (r *recordedBytes) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func (r *recordedBytes) Bytes() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.buf.Bytes()...)
}

func startRecordingProxy(t *testing.T, upstream string) (string, func() []byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	recorded := &recordedBytes{}
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			go proxyOne(client, upstream, recorded)
		}
	}()
	return ln.Addr().String(), recorded.Bytes
}

func proxyOne(client net.Conn, upstream string, recorded *recordedBytes) {
	defer client.Close()
	server, err := net.Dial("tcp", upstream)
	if err != nil {
		return
	}
	defer server.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(server, io.TeeReader(client, recorded))
		if tcp, ok := server.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, server)
		if tcp, ok := client.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
	}()
	wg.Wait()
}

// clearPasswordHandler answers the session statement Connector.Connect sends
// after a handshake. The bytes under test are written during the handshake,
// before this is called.
type clearPasswordHandler struct {
	mysql.UnimplementedHandler
	env *vtenv.Environment
}

func (h clearPasswordHandler) ComQuery(_ *mysql.Conn, _ string, callback func(*sqltypes.Result) error) error {
	return callback(&sqltypes.Result{})
}

func (h clearPasswordHandler) ComQueryMulti(_ *mysql.Conn, _ string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) error {
	return callback(sqltypes.QueryResponse{}, false, true)
}

func (h clearPasswordHandler) ComPrepare(*mysql.Conn, string) ([]*querypb.Field, uint16, error) {
	return nil, 0, nil
}

func (h clearPasswordHandler) ComStmtExecute(*mysql.Conn, *mysql.PrepareData, func(*sqltypes.Result) error) error {
	return nil
}

func (h clearPasswordHandler) ComRegisterReplica(*mysql.Conn, string, uint16, string, string) error {
	return nil
}

func (h clearPasswordHandler) ComBinlogDump(*mysql.Conn, string, uint32) error {
	return nil
}

func (h clearPasswordHandler) ComBinlogDumpGTID(*mysql.Conn, string, uint64, replication.GTIDSet, uint16) error {
	return nil
}

func (h clearPasswordHandler) WarningCount(*mysql.Conn) uint16 { return 0 }

func (h clearPasswordHandler) Env() *vtenv.Environment { return h.env }
