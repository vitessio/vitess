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

package tabletenv

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttls"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// unmanagedCleartextUser is the account Verify dials. It must not show up as
// a null-terminated string on the TCP stream.
const unmanagedCleartextUser = "vt_app"

// unmanagedServerName is the DNS name on the test server certificate.
const unmanagedServerName = "mysql.example.com"

func TestUnmanagedDefaultUseSSLRefusesUnverifiedTCP(t *testing.T) {
	fix := startUnmanagedTLSMySQL(t)
	cfg := unmanagedConfigFromFlags(t, fix, false)

	fix.capture.reset()
	err := cfg.Verify()
	raw := fix.capture.bytes()
	require.NotContains(t, string(raw), unmanagedCleartextUser+"\x00")
	require.Error(t, err)
	require.Regexp(t, `(?i)(certificate|x509)`, err.Error())
}

func TestUnmanagedDefaultUseSSLEncryptsWhenServerIsVerified(t *testing.T) {
	fix := startUnmanagedTLSMySQL(t)
	cfg := unmanagedConfigFromFlags(t, fix, true)

	fix.capture.reset()
	require.NoError(t, cfg.Verify())
	require.NotContains(t, string(fix.capture.bytes()), unmanagedCleartextUser+"\x00")
}

// unmanagedConfigFromFlags parses the vttablet database flags the way startup
// does: use-ssl stays at its default and db-ssl-mode is left unset, unless
// trustServer also passes the CA and server name.
func unmanagedConfigFromFlags(t *testing.T, fix *unmanagedTLSMySQL, trustServer bool) *TabletConfig {
	t.Helper()
	saved := dbconfigs.GlobalDBConfigs
	t.Cleanup(func() { dbconfigs.GlobalDBConfigs = saved })
	dbconfigs.GlobalDBConfigs = dbconfigs.DBConfigs{}

	registerDBFlagsOnce.Do(func() {
		dbconfigs.RegisterFlags(dbconfigs.All...)
	})
	fs := servenv.GetFlagSetFor("use-ssl-unmanaged-probe")
	fs.Init("use-ssl-unmanaged-probe", pflag.ContinueOnError)
	args := []string{
		"--db-host=" + fix.host,
		"--db-port=" + strconv.Itoa(fix.port),
		"--db-app-user=" + unmanagedCleartextUser,
		"--db-app-password=placeholder",
	}
	if trustServer {
		args = append(args, "--db-ssl-ca="+fix.caPath, "--db-server-name="+unmanagedServerName)
	}
	require.NoError(t, fs.Parse(args))
	require.True(t, dbconfigs.GlobalDBConfigs.App.UseSSL)
	require.Equal(t, unmanagedCleartextUser, dbconfigs.GlobalDBConfigs.App.User)

	cfg := NewDefaultConfig()
	cfg.Unmanaged = true
	cfg.DB = dbconfigs.GlobalDBConfigs.Clone()
	return cfg
}

var registerDBFlagsOnce sync.Once

type unmanagedTLSMySQL struct {
	host    string
	port    int
	caPath  string
	capture *unmanagedWireCapture
}

func startUnmanagedTLSMySQL(t *testing.T) *unmanagedTLSMySQL {
	t.Helper()
	handler := &unmanagedProbeHandler{env: vtenv.NewTestEnv()}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), handler, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	t.Cleanup(listener.Close)

	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", unmanagedServerName)
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		"", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	listener.TLSConfig.Store(serverConfig)
	go listener.Accept()

	backend := listener.Addr().(*net.TCPAddr)
	capture := &unmanagedWireCapture{}
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { proxy.Close() })
	go func() {
		for {
			client, err := proxy.Accept()
			if err != nil {
				return
			}
			go func(client net.Conn) {
				defer client.Close()
				server, err := net.Dial("tcp", backend.String())
				if err != nil {
					return
				}
				defer server.Close()
				done := make(chan struct{}, 2)
				go func() {
					io.Copy(server, io.TeeReader(client, capture))
					server.Close()
					done <- struct{}{}
				}()
				go func() {
					io.Copy(client, io.TeeReader(server, capture))
					client.Close()
					done <- struct{}{}
				}()
				<-done
			}(client)
		}
	}()

	proxyAddr := proxy.Addr().(*net.TCPAddr)
	return &unmanagedTLSMySQL{
		host:    proxyAddr.IP.String(),
		port:    proxyAddr.Port,
		caPath:  path.Join(root, "ca-cert.pem"),
		capture: capture,
	}
}

type unmanagedWireCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *unmanagedWireCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *unmanagedWireCapture) reset() {
	c.mu.Lock()
	c.buf.Reset()
	c.mu.Unlock()
}

func (c *unmanagedWireCapture) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.buf.Bytes()...)
}

type unmanagedProbeHandler struct {
	mysql.UnimplementedHandler
	env *vtenv.Environment
}

func (h *unmanagedProbeHandler) ComQuery(*mysql.Conn, string, func(*sqltypes.Result) error) error {
	return nil
}

func (h *unmanagedProbeHandler) ComQueryMulti(*mysql.Conn, string, func(sqltypes.QueryResponse, bool, bool) error) error {
	return nil
}

func (h *unmanagedProbeHandler) ComPrepare(*mysql.Conn, string) ([]*querypb.Field, uint16, error) {
	return nil, 0, nil
}

func (h *unmanagedProbeHandler) ComStmtExecute(*mysql.Conn, *mysql.PrepareData, func(*sqltypes.Result) error) error {
	return nil
}

func (h *unmanagedProbeHandler) ComRegisterReplica(*mysql.Conn, string, uint16, string, string) error {
	return nil
}

func (h *unmanagedProbeHandler) ComBinlogDump(*mysql.Conn, string, uint32) error { return nil }

func (h *unmanagedProbeHandler) ComBinlogDumpGTID(*mysql.Conn, string, uint64, replication.GTIDSet, uint16) error {
	return nil
}

func (h *unmanagedProbeHandler) WarningCount(*mysql.Conn) uint16 { return 0 }

func (h *unmanagedProbeHandler) Env() *vtenv.Environment { return h.env }
