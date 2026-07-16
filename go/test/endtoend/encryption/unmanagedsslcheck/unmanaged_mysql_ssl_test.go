/*
Copyright 2025 The Vitess Authors.

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

package unmanagedsslcheck

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttls"
)

func TestConnectUnamanagedMySQLRequiringSSLSuccess(t *testing.T) {
	testUser := "vt_test_user"
	connParams, cleanup, err := startMySQLWithSSLRequired(t, testUser)
	require.NoError(t, err)
	defer cleanup()

	tc1 := tabletenv.NewDefaultConfig()
	tc1.Unmanaged = true
	tc1.DB = &dbconfigs.DBConfigs{}

	connParams.Uname = testUser
	connParams.Pass = "password"
	tc1.DB.SetDbParams(connParams, mysql.ConnParams{}, mysql.ConnParams{})

	tc1.DB.Host = connParams.Host
	tc1.DB.Port = connParams.Port
	tc1.DB.Socket = ""
	tc1.DB.DBName = connParams.DbName
	tc1.DB.App.User = testUser
	tc1.DB.App.Password = "password"
	tc1.DB.SslMode = vttls.Required
	tc1.DB.SslCa = connParams.SslCa
	tc1.DB.SslCert = connParams.SslCert
	tc1.DB.SslKey = connParams.SslKey

	err = tc1.Verify()
	require.NoError(t, err)
}

func startMySQLWithSSLRequired(t *testing.T, testUser string) (mysql.ConnParams, func(), error) {
	ctx := t.Context()

	certDir := t.TempDir()
	caFile, serverCertFile, serverKeyFile := createTestSSLCerts(t, certDir)

	caBytes, err := os.ReadFile(caFile)
	require.NoError(t, err)
	certBytes, err := os.ReadFile(serverCertFile)
	require.NoError(t, err)
	keyBytes, err := os.ReadFile(serverKeyFile)
	require.NoError(t, err)

	const (
		filesDir    = "/vt/files"
		caPath      = filesDir + "/ssl-ca.pem"
		certPath    = filesDir + "/ssl-cert.pem"
		keyPath     = filesDir + "/ssl-key.pem"
		cnfPath     = filesDir + "/secure.cnf"
		initPath    = filesDir + "/init_db.sql"
		dataRoot    = "/vt/vtdataroot"
		mysqlUID    = 1
		mysqlPort   = 3306
		startupWait = 3 * time.Minute
	)

	secureCnf := strings.Join([]string{
		"[mysqld]",
		"require_secure_transport=ON",
		"mysql_native_password=ON",
		"ssl-ca=" + caPath,
		"ssl-cert=" + certPath,
		"ssl-key=" + keyPath,
		"",
	}, "\n")

	initSQL := fmt.Sprintf(`# Bootstrap for the unmanaged SSL-required mysqld.
SET GLOBAL super_read_only='OFF';
# {{custom_sql}}
CREATE DATABASE IF NOT EXISTS vttest;
CREATE USER '%[1]s'@'%%' IDENTIFIED WITH mysql_native_password BY 'password';
GRANT ALL ON *.* TO '%[1]s'@'%%';
`, testUser)

	dataDir := fmt.Sprintf("%s/vt_%010d", dataRoot, mysqlUID)
	socket := dataDir + "/mysql.sock"
	script := fmt.Sprintf(
		"mysqlctl --tablet-uid %d --mysql-port %d --log-format text init --init-db-sql-file %s && sleep infinity",
		mysqlUID, mysqlPort, initPath,
	)

	probe := []string{"mysql", "--socket", socket, "-u", testUser, "-ppassword", "-e", "SELECT 1"}

	ctr, err := testcontainers.Run(
		ctx, vitesst.Image("8.4"),
		testcontainers.WithEntrypoint("bash", "-c", script),
		testcontainers.WithEnv(map[string]string{"EXTRA_MY_CNF": cnfPath}),
		testcontainers.WithExposedPorts(fmt.Sprintf("%d/tcp", mysqlPort)),
		testcontainers.WithTmpfs(map[string]string{dataRoot: "uid=999,gid=999"}),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{Reader: strings.NewReader(string(caBytes)), ContainerFilePath: caPath, FileMode: 0o644},
			testcontainers.ContainerFile{Reader: strings.NewReader(string(certBytes)), ContainerFilePath: certPath, FileMode: 0o644},
			testcontainers.ContainerFile{Reader: strings.NewReader(string(keyBytes)), ContainerFilePath: keyPath, FileMode: 0o644},
			testcontainers.ContainerFile{Reader: strings.NewReader(secureCnf), ContainerFilePath: cnfPath, FileMode: 0o644},
			testcontainers.ContainerFile{Reader: strings.NewReader(initSQL), ContainerFilePath: initPath, FileMode: 0o644},
		),
		testcontainers.WithWaitStrategyAndDeadline(
			startupWait,
			wait.ForExec(probe).
				WithStartupTimeout(startupWait).
				WithPollInterval(100*time.Millisecond),
		),
	)
	if err != nil {
		return mysql.ConnParams{}, nil, err
	}

	cleanup := func() {
		_ = testcontainers.TerminateContainer(ctr)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		cleanup()
		return mysql.ConnParams{}, nil, err
	}
	mapped, err := ctr.MappedPort(ctx, fmt.Sprintf("%d/tcp", mysqlPort))
	if err != nil {
		cleanup()
		return mysql.ConnParams{}, nil, err
	}

	connParams := mysql.ConnParams{
		Host:    host,
		Port:    int(mapped.Num()),
		DbName:  "vttest",
		SslCa:   caFile,
		SslCert: serverCertFile,
		SslKey:  serverKeyFile,
	}
	return connParams, cleanup, nil
}

func createTestSSLCerts(t *testing.T, certDir string) (caFile, certFile, keyFile string) {
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Write CA certificate
	caFile = filepath.Join(certDir, "ca.pem")
	caCertOut, err := os.Create(caFile)
	require.NoError(t, err)
	defer caCertOut.Close()

	err = pem.Encode(caCertOut, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	require.NoError(t, err)

	// Generate client certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Client"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	certBytes, err := x509.CreateCertificate(rand.Reader, cert, ca, &certPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Write client certificate
	certFile = filepath.Join(certDir, "client.pem")
	certOut, err := os.Create(certFile)
	require.NoError(t, err)
	defer certOut.Close()

	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	require.NoError(t, err)

	// Write client key
	keyFile = filepath.Join(certDir, "client-key.pem")
	keyOut, err := os.Create(keyFile)
	require.NoError(t, err)
	defer keyOut.Close()

	err = pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey)})
	require.NoError(t, err)

	return caFile, certFile, keyFile
}
