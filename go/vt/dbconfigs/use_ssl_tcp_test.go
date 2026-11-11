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
	"crypto/tls"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttls"
	"vitess.io/vitess/go/yaml2"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// cleartextMarker is sent as a query and returned as a row. It must not
// appear on the TCP byte stream: that stream is what an on-path observer sees.
const cleartextMarker = "vitess-dbconfigs-cleartext-marker"

// serverNameOnCert is the DNS name of the test MySQL server certificate.
const serverNameOnCert = "mysql.example.com"

func TestDefaultUseSSLEncryptsAndVerifiesTCP(t *testing.T) {
	fix := startTLSMySQL(t)

	t.Run("unverified server", func(t *testing.T) {
		_, cfg := tabletDBFlags(t, fix.dialArgs(false)...)
		initTCP(cfg, fix)
		for _, connector := range allConnectors(cfg) {
			t.Run(connector.name, func(t *testing.T) {
				assertTCPVerifiesCertificate(t, fix, connector.connector)
			})
		}
	})

	t.Run("flag help", func(t *testing.T) {
		fs, cfg := tabletDBFlags(t, fix.dialArgs(false)...)
		initTCP(cfg, fix)
		assertUseSSLHelp(t, fs, cfg)
	})

	t.Run("verified server", func(t *testing.T) {
		_, cfg := tabletDBFlags(t, fix.dialArgs(true)...)
		initTCP(cfg, fix)
		for _, connector := range allConnectors(cfg) {
			t.Run(connector.name, func(t *testing.T) {
				assertTCPEncryptsSession(t, fix, connector.connector)
			})
		}
	})

	// A configured socket is ignored when the user forces TCP. The session
	// still has to be encrypted and verified.
	t.Run("use tcp with socket configured", func(t *testing.T) {
		_, cfg := tabletDBFlags(t, fix.dialArgs(false)...)
		cfg.Socket = path.Join(t.TempDir(), "mysql.sock")
		for _, userKey := range All {
			uc, _ := cfg.getParams(userKey)
			uc.UseTCP = true
		}
		initTCP(cfg, fix)
		assertTCPVerifiesCertificate(t, fix, cfg.AppWithDB())
	})
}

func TestDefaultYAMLUseSSLEncryptsAndVerifiesTCP(t *testing.T) {
	raw, err := os.ReadFile("../../../config/tablet/default.yaml")
	require.NoError(t, err)
	fix := startTLSMySQL(t)

	t.Run("unverified server", func(t *testing.T) {
		cfg := dbConfigsFromTabletYAML(t, raw, fix, false)
		for _, connector := range yamlConnectors(t, cfg) {
			t.Run(connector.name, func(t *testing.T) {
				assertTCPVerifiesCertificate(t, fix, connector.connector)
			})
		}
	})

	t.Run("verified server", func(t *testing.T) {
		cfg := dbConfigsFromTabletYAML(t, raw, fix, true)
		for _, connector := range yamlConnectors(t, cfg) {
			t.Run(connector.name, func(t *testing.T) {
				assertTCPEncryptsSession(t, fix, connector.connector)
			})
		}
	})
}

// assertTCPVerifiesCertificate expects the connector to refuse the server
// because its certificate is not one the client trusts, and to do so without
// putting a query or the account name on the TCP stream.
func assertTCPVerifiesCertificate(t *testing.T, fix *tlsMySQL, connector Connector) {
	t.Helper()
	params, err := connector.MysqlParams()
	require.NoError(t, err)
	require.Empty(t, params.UnixSocket)
	require.NotEmpty(t, params.Host)

	fix.capture.reset()
	conn, err := connector.Connect(t.Context())
	if err == nil {
		_, qerr := conn.ExecuteFetch("SELECT '"+cleartextMarker+"'", 1, true)
		conn.Close()
		require.NoError(t, qerr)
	}
	assertWireHasNoSession(t, fix.capture, params.Uname)
	require.Error(t, err)
	require.Regexp(t, `(?i)(certificate|x509)`, err.Error())
}

// assertTCPEncryptsSession expects a verified TLS session: the server reports
// TLS, and neither the setup statement, the probe query, nor its result row
// is visible on the TCP stream.
func assertTCPEncryptsSession(t *testing.T, fix *tlsMySQL, connector Connector) {
	t.Helper()
	params, err := connector.MysqlParams()
	require.NoError(t, err)
	require.Empty(t, params.UnixSocket)

	fix.capture.reset()
	conn, err := connector.Connect(t.Context())
	require.NoError(t, err)
	defer conn.Close()

	result, err := conn.ExecuteFetch("ssl echo", 1, true)
	require.NoError(t, err)
	require.Equal(t, "ON", result.Rows[0][0].ToString())

	_, err = conn.ExecuteFetch("SELECT '"+cleartextMarker+"'", 1, true)
	require.NoError(t, err)
	assertWireHasNoSession(t, fix.capture, params.Uname)
}

func assertWireHasNoSession(t *testing.T, capture *wireCapture, username string) {
	t.Helper()
	raw := capture.bytes()
	require.NotContains(t, string(raw), cleartextMarker)
	require.NotContains(t, string(raw), "@@session.sql_mode")
	require.NotContains(t, string(raw), username+"\x00")
}

func assertUseSSLHelp(t *testing.T, fs *pflag.FlagSet, cfg *DBConfigs) {
	t.Helper()
	for _, userKey := range All {
		flag := fs.Lookup("db-" + userKey + "-use-ssl")
		require.NotNil(t, flag)
		require.Equal(t, "true", flag.DefValue)
		require.Equal(t, "true", flag.Value.String())
		_, cp := cfg.getParams(userKey)
		mode := cp.EffectiveSslMode()
		if mode != vttls.VerifyCA && mode != vttls.VerifyIdentity {
			require.NotContains(t, strings.ToLower(flag.Usage), "ssl", "db-%s-use-ssl help mentions ssl while the effective mode is %q", userKey, mode)
		}
	}
}

type namedConnector struct {
	name      string
	connector Connector
}

func allConnectors(cfg *DBConfigs) []namedConnector {
	return []namedConnector{
		{"app", cfg.AppWithDB()},
		{"appdebug", cfg.AppDebugWithDB()},
		{"allprivs", cfg.AllPrivsConnector()},
		{"dba", cfg.DbaConnector()},
		{"filtered", cfg.FilteredWithDB()},
		{"repl", cfg.ReplConnector()},
		{"clone", cfg.CloneConnector()},
		{"erepl", cfg.ExternalRepl()},
	}
}

// yamlConnectors is the users whose useSsl is set in config/tablet/default.yaml.
// Clone and external repl are not in that file; the flag test covers them.
func yamlConnectors(t *testing.T, cfg *DBConfigs) []namedConnector {
	t.Helper()
	keyed := []struct {
		key       string
		connector Connector
	}{
		{App, cfg.AppWithDB()},
		{AppDebug, cfg.AppDebugWithDB()},
		{AllPrivs, cfg.AllPrivsConnector()},
		{Dba, cfg.DbaConnector()},
		{Filtered, cfg.FilteredWithDB()},
		{Repl, cfg.ReplConnector()},
	}
	out := make([]namedConnector, 0, len(keyed))
	for _, item := range keyed {
		uc, _ := cfg.getParams(item.key)
		require.True(t, uc.UseSSL, "config/tablet/default.yaml sets useSsl for %s", item.key)
		out = append(out, namedConnector{item.key, item.connector})
	}
	return out
}

func tabletDBFlags(t *testing.T, args ...string) (*pflag.FlagSet, *DBConfigs) {
	t.Helper()
	saved := GlobalDBConfigs
	t.Cleanup(func() { GlobalDBConfigs = saved })
	GlobalDBConfigs = DBConfigs{}

	fs := pflag.NewFlagSet("vttablet", pflag.ContinueOnError)
	registerBaseFlags(fs)
	for _, userKey := range All {
		uc, cp := GlobalDBConfigs.getParams(userKey)
		registerPerUserFlags(fs, userKey, uc, cp)
	}
	require.NoError(t, fs.Parse(args))
	for _, userKey := range All {
		uc, _ := GlobalDBConfigs.getParams(userKey)
		require.True(t, uc.UseSSL)
	}
	return fs, &GlobalDBConfigs
}

func initTCP(cfg *DBConfigs, fix *tlsMySQL) {
	// External repl does not inherit the global host. Give it the same TCP
	// target the other users get so its use-ssl copy is on a real dial.
	_, erepl := cfg.getParams(ExternalRepl)
	erepl.Host = fix.host
	erepl.Port = fix.port
	cfg.InitWithSocket("", collations.MySQL8())
}

func dbConfigsFromTabletYAML(t *testing.T, raw []byte, fix *tlsMySQL, trustServer bool) *DBConfigs {
	t.Helper()
	var tablet struct {
		DB DBConfigs `json:"db"`
	}
	require.NoError(t, yaml2.Unmarshal(raw, &tablet))
	cfg := tablet.DB
	cfg.Host = fix.host
	cfg.Port = fix.port
	cfg.Socket = ""
	if trustServer {
		cfg.SslCa = fix.caPath
		cfg.ServerName = serverNameOnCert
	}
	initTCP(&cfg, fix)
	return &cfg
}

type tlsMySQL struct {
	host    string
	port    int
	caPath  string
	capture *wireCapture
}

func (s *tlsMySQL) dialArgs(trustServer bool) []string {
	args := []string{
		"--db-host=" + s.host,
		"--db-port=" + strconv.Itoa(s.port),
	}
	if trustServer {
		args = append(args, "--db-ssl-ca="+s.caPath, "--db-server-name="+serverNameOnCert)
	}
	return args
}

func startTLSMySQL(t *testing.T) *tlsMySQL {
	t.Helper()
	handler := &tlsProbeHandler{env: vtenv.NewTestEnv()}
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), handler, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	t.Cleanup(listener.Close)

	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", serverNameOnCert)
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		"", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	listener.TLSConfig.Store(serverConfig)
	go listener.Accept()

	backend := listener.Addr().(*net.TCPAddr)
	capture := &wireCapture{}
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { proxy.Close() })
	go acceptCaptureProxy(proxy, backend.String(), capture)

	proxyAddr := proxy.Addr().(*net.TCPAddr)
	return &tlsMySQL{
		host:    proxyAddr.IP.String(),
		port:    proxyAddr.Port,
		caPath:  path.Join(root, "ca-cert.pem"),
		capture: capture,
	}
}

func acceptCaptureProxy(ln net.Listener, backend string, capture *wireCapture) {
	for {
		client, err := ln.Accept()
		if err != nil {
			return
		}
		go proxyOne(client, backend, capture)
	}
}

func proxyOne(client net.Conn, backend string, capture *wireCapture) {
	defer client.Close()
	server, err := net.Dial("tcp", backend)
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
}

type wireCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *wireCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *wireCapture) reset() {
	c.mu.Lock()
	c.buf.Reset()
	c.mu.Unlock()
}

func (c *wireCapture) bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.buf.Bytes()...)
}

// tlsProbeHandler is a MySQL server that answers the tablet's session-setup
// statement, reports whether the connection negotiated TLS, and echoes the
// cleartext marker as a result row.
type tlsProbeHandler struct {
	mysql.UnimplementedHandler
	env *vtenv.Environment
}

func (h *tlsProbeHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	switch {
	case query == "ssl echo":
		value := "OFF"
		if c.TLSEnabled() {
			value = "ON"
		}
		return callback(varcharResult("ssl_flag", value))
	case strings.Contains(query, cleartextMarker):
		return callback(varcharResult("marker", cleartextMarker))
	default:
		return callback(&sqltypes.Result{})
	}
}

func varcharResult(name, value string) *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: name,
			Type: querypb.Type_VARCHAR,
		}},
		Rows: [][]sqltypes.Value{{
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(value)),
		}},
	}
}

func (h *tlsProbeHandler) ComQueryMulti(*mysql.Conn, string, func(sqltypes.QueryResponse, bool, bool) error) error {
	return nil
}

func (h *tlsProbeHandler) ComPrepare(*mysql.Conn, string) ([]*querypb.Field, uint16, error) {
	return nil, 0, nil
}

func (h *tlsProbeHandler) ComStmtExecute(*mysql.Conn, *mysql.PrepareData, func(*sqltypes.Result) error) error {
	return nil
}

func (h *tlsProbeHandler) ComRegisterReplica(*mysql.Conn, string, uint16, string, string) error {
	return nil
}

func (h *tlsProbeHandler) ComBinlogDump(*mysql.Conn, string, uint32) error { return nil }

func (h *tlsProbeHandler) ComBinlogDumpGTID(*mysql.Conn, string, uint64, replication.GTIDSet, uint16) error {
	return nil
}

func (h *tlsProbeHandler) WarningCount(*mysql.Conn) uint16 { return 0 }

func (h *tlsProbeHandler) Env() *vtenv.Environment { return h.env }
